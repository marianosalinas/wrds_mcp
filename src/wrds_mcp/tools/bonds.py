"""Bond, TRACE transaction, covenant, and bond return tools for WRDS MCP.

Sources:
- trace.trace: Raw FINRA TRACE transactions (most current, needs filtering)
- wrdsapps_bondret.trace_enhanced_clean: Cleaned TRACE (research quality, ~12 month lag)
- wrdsapps_bondret.bondret: Monthly bond returns, yield, spread, duration
- fisd.fisd_mergedissue + fisd_mergedissuer: Bond characteristics
- fisd.fisd_bondholder_protective + call/put/sinking fund: Covenants
"""

import logging
from datetime import datetime
from typing import Annotated

import pandas as pd
from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field

from wrds_mcp.db.connection import get_wrds_connection, resolve_ticker_to_fisd_issuer
from wrds_mcp.tools._validation import (
    df_to_records,
    validate_cusip,
    validate_date_range,
    validate_ticker,
)

logger = logging.getLogger(__name__)

bonds_mcp = FastMCP("Bonds")

# Cutoff for routing between trace_enhanced_clean and trace.trace
# trace_enhanced_clean lags ~12 months; trace.trace is near real-time
TRACE_ENHANCED_CUTOFF_MONTHS = 12


def _should_use_raw_trace(end_date: str) -> bool:
    """Determine if date range extends beyond trace_enhanced_clean coverage."""
    end = datetime.strptime(end_date, "%Y-%m-%d")
    now = datetime.now()
    months_ago = (now.year - end.year) * 12 + (now.month - end.month)
    return months_ago < TRACE_ENHANCED_CUTOFF_MONTHS


def _issuer_ticker_filter(alias: str = "fi") -> str:
    """FISD ticker matching with issuer_id fallback.

    Matches by ticker on fisd_mergedissue, OR by issuer_id if another
    bond from the same issuer has the ticker. The :issuer_id_fallback
    param handles cases where FISD has no ticker at all (resolved via
    Compustat CUSIP linkage before the query).
    """
    return f"""(UPPER({alias}.ticker) = :ticker
               OR {alias}.issuer_id IN (
                   SELECT DISTINCT fi2.issuer_id
                   FROM fisd.fisd_mergedissue fi2
                   WHERE UPPER(fi2.ticker) = :ticker
               )
               OR (:issuer_id_fallback IS NOT NULL
                   AND {alias}.issuer_id = :issuer_id_fallback))"""


def _resolve_issuer_fallback(conn, ticker: str) -> int | None:
    """Try to resolve ticker to FISD issuer_id via Compustat CUSIP linkage.

    Used when FISD has no ticker for the issuer's bonds.
    """
    return resolve_ticker_to_fisd_issuer(conn, ticker)


def _get_company_cusips(conn, ticker: str, issuer_fb: int | None) -> list[str]:
    """Get all CUSIPs for a company from FISD (used for 144A fallback)."""
    query = f"""
        SELECT DISTINCT fi.complete_cusip
        FROM fisd.fisd_mergedissue fi
        WHERE {_issuer_ticker_filter('fi')}
    """
    try:
        df = conn.raw_sql(query, params={"ticker": ticker, "issuer_id_fallback": issuer_fb})
        return df["complete_cusip"].tolist() if not df.empty else []
    except Exception:
        return []


def _query_144a_price_history(conn, cusips: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """Query trace_btds144a for price/yield history (fallback for 144A bonds).

    Uses simple averages since the 144A volume field is text (not numeric).
    """
    if not cusips:
        return pd.DataFrame()
    placeholders = ", ".join(f":cusip_{i}" for i in range(len(cusips)))
    params = {f"cusip_{i}": c for i, c in enumerate(cusips)}
    params["start_date"] = start_date
    params["end_date"] = end_date

    query = f"""
        SELECT cusip_id AS cusip,
               trd_exctn_dt AS date,
               AVG(rptd_pr) AS avg_price,
               AVG(yld_pt) AS avg_yield,
               COUNT(*) AS num_trades
        FROM trace.trace_btds144a
        WHERE cusip_id IN ({placeholders})
          AND trd_exctn_dt BETWEEN :start_date AND :end_date
          AND trc_st NOT IN ('C', 'W')
          AND rptd_pr IS NOT NULL
        GROUP BY cusip_id, trd_exctn_dt
        ORDER BY cusip_id, trd_exctn_dt
    """
    return conn.raw_sql(query, params=params, date_cols=["date"])


def _query_144a_transactions(conn, cusips: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """Query trace_btds144a for individual transactions."""
    if not cusips:
        return pd.DataFrame()
    placeholders = ", ".join(f":cusip_{i}" for i in range(len(cusips)))
    params = {f"cusip_{i}": c for i, c in enumerate(cusips)}
    params["start_date"] = start_date
    params["end_date"] = end_date

    query = f"""
        SELECT cusip_id AS cusip,
               trd_exctn_dt AS trade_date,
               trd_exctn_tm AS trade_time,
               rptd_pr AS price,
               yld_pt AS yield_pct,
               ascii_rptd_vol_tx AS volume,
               bond_sym_id AS bond_symbol
        FROM trace.trace_btds144a
        WHERE cusip_id IN ({placeholders})
          AND trd_exctn_dt BETWEEN :start_date AND :end_date
          AND trc_st NOT IN ('C', 'W')
        ORDER BY trd_exctn_dt, trd_exctn_tm
    """
    return conn.raw_sql(query, params=params, date_cols=["trade_date"])


def _query_144a_yield_history(conn, cusip: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Query trace_btds144a for yield time series on a single CUSIP."""
    query = """
        SELECT trd_exctn_dt AS date,
               AVG(yld_pt) AS avg_yield,
               AVG(rptd_pr) AS avg_price,
               COUNT(*) AS num_trades
        FROM trace.trace_btds144a
        WHERE cusip_id = :cusip
          AND trd_exctn_dt BETWEEN :start_date AND :end_date
          AND trc_st NOT IN ('C', 'W')
          AND yld_pt IS NOT NULL
        GROUP BY trd_exctn_dt
        ORDER BY trd_exctn_dt
    """
    return conn.raw_sql(
        query,
        params={"cusip": cusip, "start_date": start_date, "end_date": end_date},
        date_cols=["date"],
    )


@bonds_mcp.tool
def get_bond_price_history(
    ticker: Annotated[str, Field(description="Company ticker symbol, e.g. 'F' for Ford")],
    start_date: Annotated[str, Field(description="Start date in YYYY-MM-DD format")],
    end_date: Annotated[str, Field(description="End date in YYYY-MM-DD format")],
    ctx: Context = None,
) -> list[dict]:
    """Get bond price/yield history for a company's bonds.

    Auto-routes between data sources based on date range:
    - trace_enhanced_clean: for historical data (research quality, cleaned reversals)
    - trace.trace: for recent data within the last ~12 months (raw, filtered)

    Returns daily volume-weighted average price and yield per CUSIP.

    Returns: list of dicts with cusip, date, avg_price, avg_yield, total_volume,
    num_trades, source.

    Example: get_bond_price_history("F", "2024-01-01", "2025-06-30")
    """
    ticker = validate_ticker(ticker)
    start_date, end_date = validate_date_range(start_date, end_date)
    conn = get_wrds_connection()
    issuer_fb = _resolve_issuer_fallback(conn, ticker)

    use_raw = _should_use_raw_trace(end_date)

    if use_raw:
        # Raw TRACE — filter out cancellations and corrections
        query = f"""
            SELECT t.cusip_id AS cusip,
                   t.trd_exctn_dt AS date,
                   SUM(t.rptd_pr * CAST(NULLIF(t.ascii_rptd_vol_tx, '') AS NUMERIC))
                       / NULLIF(SUM(CAST(NULLIF(t.ascii_rptd_vol_tx, '') AS NUMERIC)), 0) AS avg_price,
                   SUM(t.yld_pt * CAST(NULLIF(t.ascii_rptd_vol_tx, '') AS NUMERIC))
                       / NULLIF(SUM(CAST(NULLIF(t.ascii_rptd_vol_tx, '') AS NUMERIC)), 0) AS avg_yield,
                   SUM(CAST(NULLIF(t.ascii_rptd_vol_tx, '') AS NUMERIC)) AS total_volume,
                   COUNT(*) AS num_trades
            FROM trace.trace t
            INNER JOIN fisd.fisd_mergedissue fi
                ON t.cusip_id = fi.complete_cusip
            WHERE {_issuer_ticker_filter('fi')}
              AND t.trd_exctn_dt BETWEEN :start_date AND :end_date
              AND t.trc_st NOT IN ('C', 'W')
              AND t.rptd_pr IS NOT NULL
            GROUP BY t.cusip_id, t.trd_exctn_dt
            ORDER BY t.cusip_id, t.trd_exctn_dt
        """
        source = "trace.trace (raw, filtered)"
    else:
        # Cleaned TRACE — research quality
        query = f"""
            SELECT t.cusip_id AS cusip,
                   t.trd_exctn_dt AS date,
                   SUM(t.rptd_pr * t.entrd_vol_qt)
                       / NULLIF(SUM(t.entrd_vol_qt), 0) AS avg_price,
                   SUM(t.yld_pt * t.entrd_vol_qt)
                       / NULLIF(SUM(t.entrd_vol_qt), 0) AS avg_yield,
                   SUM(t.entrd_vol_qt) AS total_volume,
                   COUNT(*) AS num_trades
            FROM wrdsapps_bondret.trace_enhanced_clean t
            INNER JOIN fisd.fisd_mergedissue fi
                ON t.cusip_id = fi.complete_cusip
            WHERE {_issuer_ticker_filter('fi')}
              AND t.trd_exctn_dt BETWEEN :start_date AND :end_date
              AND t.rptd_pr IS NOT NULL
            GROUP BY t.cusip_id, t.trd_exctn_dt
            ORDER BY t.cusip_id, t.trd_exctn_dt
        """
        source = "wrdsapps_bondret.trace_enhanced_clean"

    logger.debug(
        "get_bond_price_history: ticker=%s, start=%s, end=%s, source=%s",
        ticker, start_date, end_date, source,
    )

    try:
        df = conn.raw_sql(
            query,
            params={"ticker": ticker, "start_date": start_date, "end_date": end_date, "issuer_id_fallback": issuer_fb},
            date_cols=["date"],
        )
    except Exception as e:
        raise ToolError(f"WRDS query failed: {e}")

    if df.empty:
        # Fallback: try 144A TRACE for private placements
        cusips = _get_company_cusips(conn, ticker, issuer_fb)
        try:
            df = _query_144a_price_history(conn, cusips, start_date, end_date)
        except Exception:
            df = pd.DataFrame()

        if df.empty:
            return [{"message": f"No bond price data for {ticker} between {start_date} and {end_date}.", "source": source}]
        source = "trace.trace_btds144a (144A)"

    records = df_to_records(df)
    for r in records:
        r["source"] = source
    return records


@bonds_mcp.tool
def get_bond_transactions(
    ticker: Annotated[str, Field(description="Company ticker symbol, e.g. 'AAPL'")],
    start_date: Annotated[str, Field(description="Start date in YYYY-MM-DD format")],
    end_date: Annotated[str, Field(description="End date in YYYY-MM-DD format")],
    ctx: Context = None,
) -> list[dict]:
    """Get TRACE transaction-level bond data for a company.

    Queries cleaned TRACE (trace_enhanced_clean) for individual trades.
    For very recent dates, auto-routes to raw trace.trace.

    Returns: list of dicts with cusip, trade_date, trade_time, price, yield_pct,
    volume, buy_sell, bond_symbol.

    Example: get_bond_transactions("AAPL", "2024-01-01", "2024-06-30")
    """
    ticker = validate_ticker(ticker)
    start_date, end_date = validate_date_range(start_date, end_date)
    conn = get_wrds_connection()
    issuer_fb = _resolve_issuer_fallback(conn, ticker)

    use_raw = _should_use_raw_trace(end_date)

    if use_raw:
        query = f"""
            SELECT t.cusip_id AS cusip,
                   t.trd_exctn_dt AS trade_date,
                   t.trd_exctn_tm AS trade_time,
                   t.rptd_pr AS price,
                   t.yld_pt AS yield_pct,
                   t.ascii_rptd_vol_tx AS volume,
                   t.rpt_side_cd AS buy_sell,
                   t.bond_sym_id AS bond_symbol
            FROM trace.trace t
            INNER JOIN fisd.fisd_mergedissue fi
                ON t.cusip_id = fi.complete_cusip
            WHERE {_issuer_ticker_filter('fi')}
              AND t.trd_exctn_dt BETWEEN :start_date AND :end_date
              AND t.trc_st NOT IN ('C', 'W')
            ORDER BY t.trd_exctn_dt, t.trd_exctn_tm
        """
    else:
        query = f"""
            SELECT t.cusip_id AS cusip,
                   t.trd_exctn_dt AS trade_date,
                   t.trd_exctn_tm AS trade_time,
                   t.rptd_pr AS price,
                   t.yld_pt AS yield_pct,
                   t.entrd_vol_qt AS volume,
                   t.rpt_side_cd AS buy_sell,
                   t.bond_sym_id AS bond_symbol
            FROM wrdsapps_bondret.trace_enhanced_clean t
            INNER JOIN fisd.fisd_mergedissue fi
                ON t.cusip_id = fi.complete_cusip
            WHERE {_issuer_ticker_filter('fi')}
              AND t.trd_exctn_dt BETWEEN :start_date AND :end_date
            ORDER BY t.trd_exctn_dt, t.trd_exctn_tm
        """

    logger.debug(
        "get_bond_transactions: ticker=%s, start=%s, end=%s",
        ticker, start_date, end_date,
    )

    try:
        df = conn.raw_sql(
            query,
            params={"ticker": ticker, "start_date": start_date, "end_date": end_date, "issuer_id_fallback": issuer_fb},
            date_cols=["trade_date"],
        )
    except Exception as e:
        raise ToolError(f"WRDS query failed: {e}")

    if df.empty:
        # Fallback: try 144A TRACE for private placements
        cusips = _get_company_cusips(conn, ticker, issuer_fb)
        try:
            df = _query_144a_transactions(conn, cusips, start_date, end_date)
        except Exception:
            df = pd.DataFrame()

        if df.empty:
            return [{"message": f"No TRACE transactions found for {ticker} between {start_date} and {end_date}."}]

    return df_to_records(df)


@bonds_mcp.tool
def get_bond_yield_history(
    cusip: Annotated[str, Field(description="9-character CUSIP identifier")],
    start_date: Annotated[str, Field(description="Start date in YYYY-MM-DD format")],
    end_date: Annotated[str, Field(description="End date in YYYY-MM-DD format")],
    ctx: Context = None,
) -> list[dict]:
    """Get yield time series for a specific bond by CUSIP.

    Aggregates daily volume-weighted average yield and price from TRACE.
    Auto-routes between cleaned and raw TRACE based on date range.

    Returns: list of dicts with date, avg_yield, avg_price, total_volume, num_trades.

    Example: get_bond_yield_history("037833AK6", "2024-01-01", "2024-12-31")
    """
    cusip = validate_cusip(cusip)
    start_date, end_date = validate_date_range(start_date, end_date)
    conn = get_wrds_connection()

    use_raw = _should_use_raw_trace(end_date)

    if use_raw:
        query = """
            SELECT trd_exctn_dt AS date,
                   SUM(yld_pt * CAST(NULLIF(ascii_rptd_vol_tx, '') AS NUMERIC))
                       / NULLIF(SUM(CAST(NULLIF(ascii_rptd_vol_tx, '') AS NUMERIC)), 0) AS avg_yield,
                   SUM(rptd_pr * CAST(NULLIF(ascii_rptd_vol_tx, '') AS NUMERIC))
                       / NULLIF(SUM(CAST(NULLIF(ascii_rptd_vol_tx, '') AS NUMERIC)), 0) AS avg_price,
                   SUM(CAST(NULLIF(ascii_rptd_vol_tx, '') AS NUMERIC)) AS total_volume,
                   COUNT(*) AS num_trades
            FROM trace.trace
            WHERE cusip_id = :cusip
              AND trd_exctn_dt BETWEEN :start_date AND :end_date
              AND trc_st NOT IN ('C', 'W')
              AND yld_pt IS NOT NULL
            GROUP BY trd_exctn_dt
            ORDER BY trd_exctn_dt
        """
    else:
        query = """
            SELECT trd_exctn_dt AS date,
                   SUM(yld_pt * entrd_vol_qt) / NULLIF(SUM(entrd_vol_qt), 0) AS avg_yield,
                   SUM(rptd_pr * entrd_vol_qt) / NULLIF(SUM(entrd_vol_qt), 0) AS avg_price,
                   SUM(entrd_vol_qt) AS total_volume,
                   COUNT(*) AS num_trades
            FROM wrdsapps_bondret.trace_enhanced_clean
            WHERE cusip_id = :cusip
              AND trd_exctn_dt BETWEEN :start_date AND :end_date
              AND yld_pt IS NOT NULL
            GROUP BY trd_exctn_dt
            ORDER BY trd_exctn_dt
        """

    logger.debug(
        "get_bond_yield_history: cusip=%s, start=%s, end=%s",
        cusip, start_date, end_date,
    )

    try:
        df = conn.raw_sql(
            query,
            params={"cusip": cusip, "start_date": start_date, "end_date": end_date},
            date_cols=["date"],
        )
    except Exception as e:
        raise ToolError(f"WRDS query failed: {e}")

    if df.empty:
        # Fallback: try 144A TRACE for private placements
        try:
            df = _query_144a_yield_history(conn, cusip, start_date, end_date)
        except Exception:
            df = pd.DataFrame()

        if df.empty:
            return [{"message": f"No yield data found for CUSIP {cusip} between {start_date} and {end_date}."}]

    return df_to_records(df)


@bonds_mcp.tool
def get_company_bonds(
    ticker: Annotated[str, Field(description="Company ticker symbol")],
    ctx: Context = None,
) -> list[dict]:
    """Get all outstanding bonds for a company.

    Queries fisd.fisd_mergedissue for corporate bonds matching the ticker.
    Filters out convertible, asset-backed, and exchangeable bonds.

    Returns: list of dicts with cusip, coupon, maturity, offering_amount,
    security_level, bond_type, coupon_type, offering_date.

    Example: get_company_bonds("AAPL")
    """
    ticker = validate_ticker(ticker)
    conn = get_wrds_connection()
    issuer_fb = _resolve_issuer_fallback(conn, ticker)

    query = f"""
        SELECT fi.complete_cusip AS cusip,
               fi.coupon,
               fi.maturity,
               fi.offering_amt AS offering_amount,
               fi.offering_date,
               fi.security_level,
               fi.bond_type,
               fi.coupon_type,
               fi.active_issue
        FROM fisd.fisd_mergedissue fi
        INNER JOIN fisd.fisd_mergedissuer fs
            ON fi.issuer_id = fs.issuer_id
        WHERE {_issuer_ticker_filter('fi')}
          AND fi.asset_backed = 'N'
          AND fi.convertible = 'N'
          AND fi.exchangeable = 'N'
          AND fi.bond_type IN ('CDEB', 'CMTN', 'CMTZ', 'CZ', 'USBN')
        ORDER BY fi.maturity
    """

    logger.debug("get_company_bonds: ticker=%s", ticker)

    try:
        df = conn.raw_sql(
            query,
            params={"ticker": ticker, "issuer_id_fallback": issuer_fb},
            date_cols=["maturity", "offering_date"],
        )
    except Exception as e:
        raise ToolError(f"WRDS query failed: {e}")

    if df.empty:
        return [{"message": f"No bonds found for {ticker}."}]

    return df_to_records(df)


@bonds_mcp.tool
def get_bond_returns(
    ticker: Annotated[str, Field(description="Company ticker symbol")],
    start_date: Annotated[str, Field(description="Start date in YYYY-MM-DD format")],
    end_date: Annotated[str, Field(description="End date in YYYY-MM-DD format")],
    ctx: Context = None,
) -> list[dict]:
    """Get monthly bond returns, yield, spread, and duration for a company.

    Uses wrdsapps_bondret.bondret which provides monthly bond-level analytics
    including total return, yield to maturity, option-adjusted spread,
    duration, and credit ratings.

    Returns: list of dicts with cusip, date, bond_ret, tmt_yld, oas, duration,
    price, amount_outstanding, sp_rating, moody_rating, fitch_rating.

    Example: get_bond_returns("F", "2024-01-01", "2025-12-31")
    """
    ticker = validate_ticker(ticker)
    start_date, end_date = validate_date_range(start_date, end_date)
    conn = get_wrds_connection()

    query = """
        SELECT cusip,
               date,
               ret_eom AS bond_ret,
               ROUND((yield * 100)::numeric, 4) AS bond_yield,
               ROUND((t_yld_pt * 100)::numeric, 4) AS treasury_yield,
               ROUND((t_spread * 10000)::numeric, 2) AS credit_spread,
               duration,
               price_eom AS price,
               amount_outstanding,
               r_sp AS sp_rating,
               r_mr AS moody_rating,
               r_fr AS fitch_rating,
               rating_cat,
               rating_class
        FROM wrdsapps_bondret.bondret
        WHERE UPPER(company_symbol) = :ticker
          AND date BETWEEN :start_date AND :end_date
        ORDER BY cusip, date
    """

    logger.debug(
        "get_bond_returns: ticker=%s, start=%s, end=%s",
        ticker, start_date, end_date,
    )

    try:
        df = conn.raw_sql(
            query,
            params={"ticker": ticker, "start_date": start_date, "end_date": end_date},
            date_cols=["date"],
        )
    except Exception as e:
        raise ToolError(f"WRDS query failed: {e}")

    if df.empty:
        return [{"message": f"No bond return data for {ticker} between {start_date} and {end_date}."}]

    return df_to_records(df)


@bonds_mcp.tool
def get_bond_covenants(
    ticker: Annotated[str, Field(description="Company ticker symbol")],
    ctx: Context = None,
) -> dict:
    """Get bond covenant details for a company from Mergent FISD.

    Queries bondholder protective covenants, call schedule, put schedule,
    and sinking fund provisions. Groups results by bond (CUSIP).

    Returns: dict with ticker, bonds (list of covenant details per bond),
    and summary counts.

    Example: get_bond_covenants("F")
    """
    ticker = validate_ticker(ticker)
    conn = get_wrds_connection()
    issuer_fb = _resolve_issuer_fallback(conn, ticker)

    # First get the company's bond CUSIPs and issue_ids
    cusip_query = f"""
        SELECT fi.issue_id, fi.complete_cusip, fi.coupon, fi.maturity, fi.issuer_id
        FROM fisd.fisd_mergedissue fi
        WHERE {_issuer_ticker_filter('fi')}
          AND fi.asset_backed = 'N'
          AND fi.convertible = 'N'
          AND fi.bond_type IN ('CDEB', 'CMTN', 'CMTZ', 'CZ', 'USBN')
        ORDER BY fi.maturity
    """

    logger.debug("get_bond_covenants: ticker=%s", ticker)

    try:
        bonds_df = conn.raw_sql(cusip_query, params={"ticker": ticker, "issuer_id_fallback": issuer_fb}, date_cols=["maturity"])
    except Exception as e:
        raise ToolError(f"WRDS query failed: {e}")

    if bonds_df.empty:
        return {"ticker": ticker, "message": "No bonds found.", "bonds": []}

    issuer_ids = bonds_df["issuer_id"].unique().tolist()
    if not issuer_ids:
        return {"ticker": ticker, "message": "No issuer found.", "bonds": []}

    # Get issue_ids for covenant lookups
    issue_ids = bonds_df["issue_id"].dropna().astype(int).unique().tolist()
    if not issue_ids:
        return {"ticker": ticker, "message": "No issue IDs found.", "bonds": []}

    # Bondholder protective covenants (keyed by issue_id)
    cov_query = """
        SELECT issue_id,
               cross_default, cross_acceleration,
               change_control_put_provisions,
               rating_decline_trigger_put,
               negative_pledge_covenant,
               after_acquired_property_clause,
               asset_sale_clause
        FROM fisd.fisd_bondholder_protective
        WHERE issue_id = ANY(:issue_ids)
    """

    # Call schedule
    call_query = """
        SELECT issue_id, call_date, call_price
        FROM fisd.fisd_call_schedule
        WHERE issue_id = ANY(:issue_ids)
        ORDER BY issue_id, call_date
    """

    # Put schedule
    put_query = """
        SELECT issue_id, put_date, put_price
        FROM fisd.fisd_put_schedule
        WHERE issue_id = ANY(:issue_ids)
        ORDER BY issue_id, put_date
    """

    # Sinking fund
    sink_query = """
        SELECT issue_id
        FROM fisd.fisd_sinking_fund
        WHERE issue_id = ANY(:issue_ids)
    """

    try:
        cov_df = conn.raw_sql(cov_query, params={"issue_ids": issue_ids})
        call_df = conn.raw_sql(call_query, params={"issue_ids": issue_ids}, date_cols=["call_date"])
        put_df = conn.raw_sql(put_query, params={"issue_ids": issue_ids}, date_cols=["put_date"])
        sink_df = conn.raw_sql(sink_query, params={"issue_ids": issue_ids})
    except Exception as e:
        raise ToolError(f"WRDS covenant query failed: {e}")

    # Build per-bond covenant summary
    bond_results = []
    for _, bond in bonds_df.iterrows():
        cusip = bond["complete_cusip"]
        issue_id = int(bond["issue_id"]) if pd.notna(bond.get("issue_id")) else None
        coupon = float(bond["coupon"]) if pd.notna(bond.get("coupon")) else None
        maturity = bond["maturity"].isoformat()[:10] if hasattr(bond["maturity"], "isoformat") else str(bond["maturity"])

        entry = {
            "cusip": cusip,
            "coupon": coupon,
            "maturity": maturity,
        }

        # Protective covenants
        bond_covs = cov_df[cov_df["issue_id"] == issue_id] if issue_id else pd.DataFrame()
        if not bond_covs.empty:
            row = bond_covs.iloc[0]
            entry["covenants"] = {
                "cross_default": row.get("cross_default"),
                "cross_acceleration": row.get("cross_acceleration"),
                "change_of_control_put": row.get("change_control_put_provisions"),
                "rating_decline_put": row.get("rating_decline_trigger_put"),
                "negative_pledge": row.get("negative_pledge_covenant"),
                "asset_sale_clause": row.get("asset_sale_clause"),
                "after_acquired_property": row.get("after_acquired_property_clause"),
            }
        else:
            entry["covenants"] = None

        # Call schedule
        bond_calls = call_df[call_df["issue_id"] == issue_id] if issue_id else pd.DataFrame()
        if not bond_calls.empty:
            entry["call_schedule"] = df_to_records(bond_calls[["call_date", "call_price"]])
        else:
            entry["call_schedule"] = []

        # Put schedule
        bond_puts = put_df[put_df["issue_id"] == issue_id] if issue_id else pd.DataFrame()
        if not bond_puts.empty:
            entry["put_schedule"] = df_to_records(bond_puts[["put_date", "put_price"]])
        else:
            entry["put_schedule"] = []

        # Sinking fund
        bond_sink = sink_df[sink_df["issue_id"] == issue_id] if issue_id else pd.DataFrame()
        entry["has_sinking_fund"] = not bond_sink.empty

        bond_results.append(entry)

    return {
        "ticker": ticker,
        "total_bonds": len(bond_results),
        "bonds_with_covenants": sum(1 for b in bond_results if b.get("covenants")),
        "bonds_with_calls": sum(1 for b in bond_results if b.get("call_schedule")),
        "bonds_with_puts": sum(1 for b in bond_results if b.get("put_schedule")),
        "bonds": bond_results,
    }
