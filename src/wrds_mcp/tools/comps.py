"""Comps table tool for WRDS MCP — side-by-side issuer comparison."""

import logging
from typing import Annotated

import pandas as pd
from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field

from wrds_mcp.db.connection import get_wrds_connection
from wrds_mcp.tools._validation import validate_ticker

logger = logging.getLogger(__name__)

comps_mcp = FastMCP("Comps")

FUNDA_FILTER = "indfmt = 'INDL' AND datafmt = 'STD' AND consol = 'C'"


@comps_mcp.tool
def get_comps_table(
    tickers: Annotated[list[str], Field(description="List of ticker symbols to compare, e.g. ['F', 'GM', 'STLA']")],
    ctx: Context = None,
) -> dict:
    """Build a side-by-side credit comps table for a list of issuers.

    For each ticker, pulls: credit ratings (S&P, Moody's, Fitch), latest annual
    financials (revenue, EBITDA, total debt, net debt, leverage, interest coverage,
    market cap), outstanding bond count and total amount, and equity returns
    (1mo, 3mo, 6mo, 12mo).

    Returns: dict with tickers list and comps list (one dict per issuer), plus
    the as_of_date for bond/rating data.

    Example: get_comps_table(["F", "GM", "STLA", "HMC"])
    """
    from datetime import datetime
    from dateutil.relativedelta import relativedelta

    if not tickers or len(tickers) == 0:
        raise ToolError("Must provide at least one ticker.")
    if len(tickers) > 20:
        raise ToolError("Maximum 20 tickers per comps table.")

    clean_tickers = [validate_ticker(t) for t in tickers]
    conn = get_wrds_connection()

    # Detect latest bondret month
    from wrds_mcp.tools.screening import _detect_latest_full_month
    try:
        latest_month = _detect_latest_full_month(conn)
    except ToolError:
        latest_month = None

    latest_dt = datetime.strptime(latest_month, "%Y-%m-%d") if latest_month else datetime.now()

    # Build ticker list for SQL
    ticker_list = ", ".join(f"'{t}'" for t in clean_tickers)

    # 1. Ratings from bondret
    ratings_df = pd.DataFrame()
    if latest_month:
        try:
            ratings_df = conn.raw_sql(f"""
                SELECT DISTINCT ON (company_symbol)
                    company_symbol AS ticker,
                    r_sp AS sp_rating,
                    r_mr AS moody_rating,
                    r_fr AS fitch_rating,
                    rating_cat,
                    rating_class
                FROM wrdsapps_bondret.bondret
                WHERE date = :latest_month
                  AND UPPER(company_symbol) IN ({ticker_list})
                ORDER BY company_symbol, amount_outstanding DESC NULLS LAST
            """, params={"latest_month": latest_month})
        except Exception as e:
            logger.warning("Ratings query failed: %s", e)

    # 2. Financials from Compustat
    try:
        fin_df = conn.raw_sql(f"""
            SELECT DISTINCT ON (s.tic)
                s.tic AS ticker,
                f.conm AS company_name,
                f.sich AS sic_code,
                ROUND(f.sale::numeric, 0) AS revenue,
                ROUND(f.oibdp::numeric, 0) AS ebitda,
                ROUND((COALESCE(f.dltt, 0) + COALESCE(f.dlc, 0))::numeric, 0) AS total_debt,
                ROUND((COALESCE(f.dltt, 0) + COALESCE(f.dlc, 0) - COALESCE(f.che, 0))::numeric, 0) AS net_debt,
                ROUND(f.che::numeric, 0) AS cash,
                ROUND(((COALESCE(f.dltt, 0) + COALESCE(f.dlc, 0)) / NULLIF(f.oibdp, 0))::numeric, 2) AS leverage,
                ROUND((f.oibdp / NULLIF(f.xint, 0))::numeric, 2) AS interest_coverage,
                ROUND((COALESCE(f.csho, 0) * COALESCE(f.prcc_f, 0))::numeric, 0) AS market_cap,
                f.datadate::text AS financials_date
            FROM comp.funda f
            INNER JOIN comp.security s ON s.gvkey = f.gvkey
            WHERE {FUNDA_FILTER}
              AND UPPER(s.tic) IN ({ticker_list})
              AND f.datadate >= CURRENT_DATE - INTERVAL '2 years'
            ORDER BY s.tic, f.datadate DESC
        """)
    except Exception as e:
        logger.warning("Financials query failed: %s", e)
        fin_df = pd.DataFrame()

    # 3. Bond summary from bondret
    bonds_df = pd.DataFrame()
    if latest_month:
        try:
            bonds_df = conn.raw_sql(f"""
                SELECT
                    company_symbol AS ticker,
                    COUNT(*) AS bond_count,
                    ROUND(SUM(amount_outstanding)::numeric, 0) AS total_amount_outstanding,
                    ROUND((AVG(t_spread) * 10000)::numeric, 1) AS avg_spread,
                    ROUND((AVG(yield) * 100)::numeric, 3) AS avg_yield,
                    ROUND(AVG(duration)::numeric, 2) AS avg_duration
                FROM wrdsapps_bondret.bondret
                WHERE date = :latest_month
                  AND UPPER(company_symbol) IN ({ticker_list})
                GROUP BY company_symbol
            """, params={"latest_month": latest_month})
        except Exception as e:
            logger.warning("Bond summary query failed: %s", e)

    # 4. Equity returns from CRSP
    date_1mo = (latest_dt - relativedelta(months=1)).strftime("%Y-%m-%d")
    date_3mo = (latest_dt - relativedelta(months=3)).strftime("%Y-%m-%d")
    date_6mo = (latest_dt - relativedelta(months=6)).strftime("%Y-%m-%d")
    date_12mo = (latest_dt - relativedelta(months=12)).strftime("%Y-%m-%d")
    ref_date = latest_month or latest_dt.strftime("%Y-%m-%d")

    try:
        eq_df = conn.raw_sql(f"""
            SELECT ticker,
                   EXP(SUM(LN(1 + mthret)) FILTER (WHERE mthcaldt >= :date_1mo ::date)) - 1 AS ret_1mo,
                   EXP(SUM(LN(1 + mthret)) FILTER (WHERE mthcaldt >= :date_3mo ::date)) - 1 AS ret_3mo,
                   EXP(SUM(LN(1 + mthret)) FILTER (WHERE mthcaldt >= :date_6mo ::date)) - 1 AS ret_6mo,
                   EXP(SUM(LN(1 + mthret)) FILTER (WHERE mthcaldt >= :date_12mo ::date)) - 1 AS ret_12mo
            FROM crsp.msf_v2
            WHERE mthcaldt >= :date_12mo ::date
              AND mthcaldt <= :ref_date ::date
              AND UPPER(ticker) IN ({ticker_list})
              AND mthret IS NOT NULL
              AND mthret > -1
            GROUP BY ticker
        """, params={
            "date_1mo": date_1mo,
            "date_3mo": date_3mo,
            "date_6mo": date_6mo,
            "date_12mo": date_12mo,
            "ref_date": ref_date,
        })
    except Exception as e:
        logger.warning("Equity returns query failed: %s", e)
        eq_df = pd.DataFrame()

    # Merge everything by ticker
    comps = []
    for ticker in clean_tickers:
        entry: dict = {"ticker": ticker}

        # Ratings
        if not ratings_df.empty:
            r = ratings_df[ratings_df["ticker"].str.upper() == ticker]
            if not r.empty:
                row = r.iloc[0]
                entry["sp_rating"] = row.get("sp_rating") if pd.notna(row.get("sp_rating")) else None
                entry["moody_rating"] = row.get("moody_rating") if pd.notna(row.get("moody_rating")) else None
                entry["fitch_rating"] = row.get("fitch_rating") if pd.notna(row.get("fitch_rating")) else None
                entry["rating_class"] = row.get("rating_class") if pd.notna(row.get("rating_class")) else None

        # Financials
        if not fin_df.empty:
            f = fin_df[fin_df["ticker"].str.upper() == ticker]
            if not f.empty:
                row = f.iloc[0]
                for col in ["company_name", "revenue", "ebitda", "total_debt", "net_debt",
                            "cash", "leverage", "interest_coverage", "market_cap", "financials_date"]:
                    val = row.get(col)
                    entry[col] = float(val) if isinstance(val, (int, float)) and pd.notna(val) else (
                        str(val) if pd.notna(val) else None
                    )

        # Bonds
        if not bonds_df.empty:
            b = bonds_df[bonds_df["ticker"].str.upper() == ticker]
            if not b.empty:
                row = b.iloc[0]
                entry["bond_count"] = int(row["bond_count"])
                entry["total_amount_outstanding"] = float(row["total_amount_outstanding"]) if pd.notna(row.get("total_amount_outstanding")) else None
                entry["avg_spread_bps"] = float(row["avg_spread"]) if pd.notna(row.get("avg_spread")) else None
                entry["avg_yield"] = float(row["avg_yield"]) if pd.notna(row.get("avg_yield")) else None
                entry["avg_duration"] = float(row["avg_duration"]) if pd.notna(row.get("avg_duration")) else None

        # Equity returns
        if not eq_df.empty:
            e = eq_df[eq_df["ticker"].str.upper() == ticker]
            if not e.empty:
                row = e.iloc[0]
                entry["equity_return_1mo"] = round(float(row["ret_1mo"]), 4) if pd.notna(row.get("ret_1mo")) else None
                entry["equity_return_3mo"] = round(float(row["ret_3mo"]), 4) if pd.notna(row.get("ret_3mo")) else None
                entry["equity_return_6mo"] = round(float(row["ret_6mo"]), 4) if pd.notna(row.get("ret_6mo")) else None
                entry["equity_return_12mo"] = round(float(row["ret_12mo"]), 4) if pd.notna(row.get("ret_12mo")) else None

        comps.append(entry)

    return {
        "as_of_date": latest_month,
        "tickers": clean_tickers,
        "count": len(comps),
        "comps": comps,
    }
