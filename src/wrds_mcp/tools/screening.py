"""Screening tools for WRDS MCP — find issuers and bonds matching criteria."""

import logging
from typing import Annotated

import pandas as pd
from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field

from wrds_mcp.db.connection import get_wrds_connection
from wrds_mcp.tools._validation import df_to_records, validate_date, validate_date_range, validate_ticker

logger = logging.getLogger(__name__)

screening_mcp = FastMCP("Screening")

# --- Constants ---

FUNDA_FILTER = "indfmt = 'INDL' AND datafmt = 'STD' AND consol = 'C'"

# S&P-style rating to numeric (lower = better). Matches bondret rating_cat scale.
RATING_TO_NUM = {
    "AAA": 1, "AA+": 2, "AA": 3, "AA-": 4,
    "A+": 5, "A": 6, "A-": 7,
    "BBB+": 8, "BBB": 9, "BBB-": 10,
    "BB+": 11, "BB": 12, "BB-": 13,
    "B+": 14, "B": 15, "B-": 16,
    "CCC+": 17, "CCC": 18, "CCC-": 19,
    "CC": 20, "C": 21, "D": 22,
}

# Sector → SIC code ranges
SECTOR_SIC_RANGES = {
    "Energy": [(1300, 1399), (2911, 2911), (2990, 2999), (4922, 4925)],
    "Materials": [(1000, 1299), (1400, 1499), (2600, 2699), (2800, 2829), (2860, 2899), (3200, 3399)],
    "Industrials": [(1500, 1799), (3400, 3599), (3700, 3719), (3720, 3729), (3740, 3799), (4000, 4299), (4400, 4599), (4700, 4799)],
    "Consumer Discretionary": [(2300, 2399), (2500, 2599), (3140, 3199), (3600, 3639), (3651, 3699), (3711, 3716), (5000, 5199), (5300, 5599), (5700, 5799), (5900, 5999), (7000, 7099), (7200, 7299), (7800, 7999)],
    "Consumer Staples": [(2000, 2199), (5100, 5159), (5400, 5499)],
    "Healthcare": [(2830, 2836), (3693, 3693), (3840, 3859), (5047, 5047), (5122, 5122), (8000, 8099)],
    "Financials": [(6000, 6411), (6500, 6799)],
    "Technology": [(3570, 3599), (3660, 3692), (3694, 3699), (5045, 5045), (5065, 5065), (7370, 7379)],
    "Telecom": [(4800, 4899)],
    "Utilities": [(4900, 4999)],
    "Real Estate": [(6500, 6599)],
}

# Cache for latest full bondret month
_latest_full_month: str | None = None


# --- Helpers ---

def _detect_latest_full_month(conn) -> str:
    """Find the most recent bondret month with >500 distinct issuers."""
    global _latest_full_month
    if _latest_full_month is not None:
        return _latest_full_month

    df = conn.raw_sql("""
        SELECT date, COUNT(DISTINCT company_symbol) AS n
        FROM wrdsapps_bondret.bondret
        WHERE date >= CURRENT_DATE - INTERVAL '24 months'
        GROUP BY date
        HAVING COUNT(DISTINCT company_symbol) > 500
        ORDER BY date DESC
        LIMIT 1
    """)
    if df.empty:
        raise ToolError("Could not detect a recent bondret month with full coverage.")

    _latest_full_month = str(df.iloc[0]["date"])[:10]
    logger.info("Detected latest full bondret month: %s", _latest_full_month)
    return _latest_full_month


def _validate_rating(rating: str) -> int:
    """Convert S&P-style rating to numeric. Lower = better."""
    rating = rating.strip().upper()
    if rating not in RATING_TO_NUM:
        valid = ", ".join(RATING_TO_NUM.keys())
        raise ToolError(f"Invalid rating '{rating}'. Valid ratings: {valid}")
    return RATING_TO_NUM[rating]


RATING_CAT_TO_NUM_SQL = """
    CASE b.rating_cat
        WHEN 'AAA' THEN 1
        WHEN 'AA' THEN 3
        WHEN 'A' THEN 6
        WHEN 'BBB' THEN 9
        WHEN 'BB' THEN 12
        WHEN 'B' THEN 15
        WHEN 'CCC' THEN 18
        WHEN 'CC' THEN 20
        WHEN 'C' THEN 21
        WHEN 'D' THEN 22
        ELSE NULL
    END
""".strip()


def _build_sic_filter(ranges: list[tuple[int, int]], params: dict, col: str = "f.sich") -> str:
    """Build SQL OR clause for SIC code ranges."""
    clauses = []
    for i, (lo, hi) in enumerate(ranges):
        params[f"sic_lo_{i}"] = lo
        params[f"sic_hi_{i}"] = hi
        clauses.append(f"{col} BETWEEN :sic_lo_{i} AND :sic_hi_{i}")
    return "(" + " OR ".join(clauses) + ")"


# --- Tool 1: screen_issuers ---

@screening_mcp.tool
def screen_issuers(
    rating_class: Annotated[str | None, Field(description="'HY' or 'IG'")] = None,
    min_rating: Annotated[str | None, Field(description="Best rating in range (e.g. 'A-'). Uses S&P scale.")] = None,
    max_rating: Annotated[str | None, Field(description="Worst rating in range (e.g. 'BB'). Uses S&P scale.")] = None,
    sector: Annotated[str | None, Field(description="Sector: Energy, Materials, Industrials, Consumer Discretionary, Consumer Staples, Healthcare, Financials, Technology, Telecom, Utilities, Real Estate")] = None,
    min_market_cap: Annotated[float | None, Field(description="Minimum market cap in millions USD")] = None,
    max_market_cap: Annotated[float | None, Field(description="Maximum market cap in millions USD")] = None,
    min_ebitda: Annotated[float | None, Field(description="Minimum EBITDA in millions USD")] = None,
    max_leverage: Annotated[float | None, Field(description="Maximum Debt/EBITDA ratio")] = None,
    min_leverage: Annotated[float | None, Field(description="Minimum Debt/EBITDA ratio")] = None,
    sort_by: Annotated[str, Field(description="Sort column: market_cap, ebitda, leverage, sp_rating")] = "market_cap",
    limit: Annotated[int, Field(description="Max results", ge=1, le=500)] = 100,
    ctx: Context = None,
) -> dict:
    """Screen for issuers matching credit, financial, and sector criteria.

    All filters are optional — combine any subset. Rating data comes from
    wrdsapps_bondret (auto-detects latest full month). Financials from Compustat.
    Equity returns from CRSP monthly.

    min_rating = best (highest quality) end of range, e.g. 'A-' means 'A- or worse'.
    max_rating = worst (lowest quality) end of range, e.g. 'BB' means 'BB or better'.
    So min_rating='BBB+', max_rating='BB-' gives the BB-to-BBB+ crossover range.

    Returns: dict with as_of_date, filters_applied, result_count, issuers (list).

    Example: screen_issuers(rating_class="HY", sector="Energy", min_market_cap=5000)
    """
    from datetime import datetime
    from dateutil.relativedelta import relativedelta

    conn = get_wrds_connection()
    latest_month = _detect_latest_full_month(conn)
    latest_dt = datetime.strptime(latest_month, "%Y-%m-%d")

    params: dict = {
        "latest_month": latest_month,
        "date_1mo": (latest_dt - relativedelta(months=1)).strftime("%Y-%m-%d"),
        "date_3mo": (latest_dt - relativedelta(months=3)).strftime("%Y-%m-%d"),
        "date_6mo": (latest_dt - relativedelta(months=6)).strftime("%Y-%m-%d"),
        "date_12mo": (latest_dt - relativedelta(months=12)).strftime("%Y-%m-%d"),
    }
    rating_filters: list[str] = []
    fin_filters: list[str] = []
    filters_applied: dict = {}

    # --- Rating filters ---
    if rating_class is not None:
        rc = rating_class.strip().upper()
        if rc not in ("HY", "IG"):
            raise ToolError("rating_class must be 'HY' or 'IG'.")
        # bondret uses '0.IG' and '1.HY'
        params["rating_class"] = f"{'1' if rc == 'HY' else '0'}.{rc}"
        rating_filters.append("b.rating_class = :rating_class")
        filters_applied["rating_class"] = rc

    if min_rating is not None:
        num = _validate_rating(min_rating)
        params["min_rating_num"] = num
        rating_filters.append(f"({RATING_CAT_TO_NUM_SQL}) >= :min_rating_num")
        filters_applied["min_rating"] = min_rating

    if max_rating is not None:
        num = _validate_rating(max_rating)
        params["max_rating_num"] = num
        rating_filters.append(f"({RATING_CAT_TO_NUM_SQL}) <= :max_rating_num")
        filters_applied["max_rating"] = max_rating

    # --- Financial filters ---
    if sector is not None:
        sector_clean = sector.strip().title()
        if sector_clean not in SECTOR_SIC_RANGES:
            valid = ", ".join(sorted(SECTOR_SIC_RANGES.keys()))
            raise ToolError(f"Unknown sector '{sector}'. Valid sectors: {valid}")
        sic_clause = _build_sic_filter(SECTOR_SIC_RANGES[sector_clean], params, col="fin.sic_code")
        fin_filters.append(sic_clause)
        filters_applied["sector"] = sector_clean

    if min_market_cap is not None:
        params["min_mktcap"] = min_market_cap
        fin_filters.append("fin.market_cap >= :min_mktcap")
        filters_applied["min_market_cap"] = min_market_cap

    if max_market_cap is not None:
        params["max_mktcap"] = max_market_cap
        fin_filters.append("fin.market_cap <= :max_mktcap")
        filters_applied["max_market_cap"] = max_market_cap

    if min_ebitda is not None:
        params["min_ebitda"] = min_ebitda
        fin_filters.append("fin.ebitda >= :min_ebitda")
        filters_applied["min_ebitda"] = min_ebitda

    if max_leverage is not None:
        params["max_leverage"] = max_leverage
        fin_filters.append("fin.leverage <= :max_leverage")
        fin_filters.append("fin.leverage IS NOT NULL")
        filters_applied["max_leverage"] = max_leverage

    if min_leverage is not None:
        params["min_leverage"] = min_leverage
        fin_filters.append("fin.leverage >= :min_leverage")
        filters_applied["min_leverage"] = min_leverage

    # --- Build sort ---
    valid_sorts = {"market_cap", "ebitda", "leverage", "sp_rating", "ticker"}
    if sort_by not in valid_sorts:
        sort_by = "market_cap"
    sort_col = {
        "market_cap": "fin.market_cap DESC NULLS LAST",
        "ebitda": "fin.ebitda DESC NULLS LAST",
        "leverage": "fin.leverage ASC NULLS LAST",
        "sp_rating": "br.rating_num ASC NULLS LAST",
        "ticker": "br.company_symbol ASC",
    }[sort_by]

    rating_where = " AND ".join(rating_filters) if rating_filters else "TRUE"
    fin_where = " AND ".join(fin_filters) if fin_filters else "TRUE"

    params["limit"] = limit

    query = f"""
        WITH bond_ratings AS (
            SELECT DISTINCT ON (b.company_symbol)
                b.company_symbol,
                b.r_sp AS sp_rating,
                b.r_mr AS moody_rating,
                b.r_fr AS fitch_rating,
                b.rating_cat,
                b.rating_class,
                {RATING_CAT_TO_NUM_SQL} AS rating_num
            FROM wrdsapps_bondret.bondret b
            LEFT JOIN fisd.fisd_mergedissue fi
                ON b.cusip = fi.complete_cusip
            WHERE b.date = :latest_month
              AND b.company_symbol IS NOT NULL
              AND {rating_where}
            ORDER BY b.company_symbol,
                     CASE WHEN fi.security_level = 'SU' THEN 1
                          WHEN fi.security_level = 'SEN' THEN 2
                          WHEN fi.security_level = 'SS' THEN 3
                          ELSE 4 END,
                     b.amount_outstanding DESC NULLS LAST
        ),
        financials AS (
            SELECT DISTINCT ON (f.gvkey)
                f.gvkey,
                s.tic AS ticker,
                f.conm AS company_name,
                f.sich AS sic_code,
                ROUND((COALESCE(f.csho, 0) * COALESCE(f.prcc_f, 0))::numeric, 0) AS market_cap,
                ROUND(f.sale::numeric, 0) AS revenue,
                ROUND(f.oibdp::numeric, 0) AS ebitda,
                ROUND((COALESCE(f.dltt, 0) + COALESCE(f.dlc, 0))::numeric, 0) AS total_debt,
                ROUND((COALESCE(f.dltt, 0) + COALESCE(f.dlc, 0) - COALESCE(f.che, 0))::numeric, 0) AS net_debt,
                ROUND(((COALESCE(f.dltt, 0) + COALESCE(f.dlc, 0)) / NULLIF(f.oibdp, 0))::numeric, 2) AS leverage,
                ROUND((f.oibdp / NULLIF(f.xint, 0))::numeric, 2) AS interest_coverage,
                f.datadate
            FROM comp.funda f
            INNER JOIN comp.security s ON s.gvkey = f.gvkey
            WHERE {FUNDA_FILTER}
              AND f.datadate >= CURRENT_DATE - INTERVAL '2 years'
            ORDER BY f.gvkey, f.datadate DESC
        ),
        equity_returns AS (
            SELECT ticker,
                   EXP(SUM(LN(1 + mthret)) FILTER (WHERE mthcaldt >= :date_1mo ::date)) - 1 AS ret_1mo,
                   EXP(SUM(LN(1 + mthret)) FILTER (WHERE mthcaldt >= :date_3mo ::date)) - 1 AS ret_3mo,
                   EXP(SUM(LN(1 + mthret)) FILTER (WHERE mthcaldt >= :date_6mo ::date)) - 1 AS ret_6mo,
                   EXP(SUM(LN(1 + mthret)) FILTER (WHERE mthcaldt >= :date_12mo ::date)) - 1 AS ret_12mo
            FROM crsp.msf_v2
            WHERE mthcaldt >= :date_12mo ::date
              AND mthcaldt <= :latest_month ::date
              AND mthret IS NOT NULL
              AND mthret > -1
            GROUP BY ticker
        )
        SELECT
            br.company_symbol AS ticker,
            fin.company_name,
            br.sp_rating,
            br.moody_rating,
            br.fitch_rating,
            br.rating_class,
            br.rating_cat,
            fin.sic_code,
            fin.market_cap,
            fin.revenue,
            fin.ebitda,
            fin.total_debt,
            fin.net_debt,
            fin.leverage,
            fin.interest_coverage,
            fin.datadate::text AS financials_date,
            ROUND(er.ret_1mo::numeric, 4) AS equity_return_1mo,
            ROUND(er.ret_3mo::numeric, 4) AS equity_return_3mo,
            ROUND(er.ret_6mo::numeric, 4) AS equity_return_6mo,
            ROUND(er.ret_12mo::numeric, 4) AS equity_return_12mo
        FROM bond_ratings br
        INNER JOIN financials fin
            ON UPPER(fin.ticker) = UPPER(br.company_symbol)
        LEFT JOIN equity_returns er
            ON UPPER(er.ticker) = UPPER(br.company_symbol)
        WHERE {fin_where}
        ORDER BY {sort_col}
        LIMIT :limit
    """

    logger.info("screen_issuers: filters=%s", filters_applied)

    try:
        df = conn.raw_sql(query, params=params)
    except Exception as e:
        raise ToolError(f"Screening query failed: {e}")

    if df.empty:
        return {
            "as_of_date": latest_month,
            "filters_applied": filters_applied,
            "result_count": 0,
            "issuers": [{"message": "No issuers match the specified criteria."}],
        }

    records = df_to_records(df)

    return {
        "as_of_date": latest_month,
        "filters_applied": filters_applied,
        "result_count": len(records),
        "issuers": records,
    }


# --- Tool 2: screen_bonds ---

@screening_mcp.tool
def screen_bonds(
    ticker: Annotated[str | None, Field(description="Filter to a specific issuer")] = None,
    rating_class: Annotated[str | None, Field(description="'HY' or 'IG'")] = None,
    min_rating: Annotated[str | None, Field(description="Best rating in range (e.g. 'A-'). Uses S&P scale.")] = None,
    max_rating: Annotated[str | None, Field(description="Worst rating in range (e.g. 'BB'). Uses S&P scale.")] = None,
    security_level: Annotated[str | None, Field(description="'SU' (Senior Unsecured), 'SS' (Senior Secured), 'SEN' (Senior), 'SUB' (Subordinated)")] = None,
    min_amount_outstanding: Annotated[float | None, Field(description="Minimum amount outstanding in millions")] = None,
    min_coupon: Annotated[float | None, Field(description="Minimum coupon rate (%)")] = None,
    max_coupon: Annotated[float | None, Field(description="Maximum coupon rate (%)")] = None,
    maturity_after: Annotated[str | None, Field(description="Only bonds maturing after this date (YYYY-MM-DD)")] = None,
    maturity_before: Annotated[str | None, Field(description="Only bonds maturing before this date (YYYY-MM-DD)")] = None,
    min_spread: Annotated[float | None, Field(description="Minimum treasury spread (bps)")] = None,
    max_spread: Annotated[float | None, Field(description="Maximum treasury spread (bps)")] = None,
    sector: Annotated[str | None, Field(description="Issuer sector: Energy, Healthcare, etc.")] = None,
    sort_by: Annotated[str, Field(description="Sort: spread, yield, amount_outstanding, maturity, coupon")] = "spread",
    limit: Annotated[int, Field(description="Max results", ge=1, le=1000)] = 200,
    ctx: Context = None,
) -> dict:
    """Screen for individual bonds matching criteria.

    All filters are optional. Data from wrdsapps_bondret (latest full month)
    joined with FISD for bond characteristics and Compustat for sector.

    min_rating = best (highest quality) end, max_rating = worst end.

    Returns: dict with as_of_date, filters_applied, result_count, bonds (list).

    Example: screen_bonds(rating_class="HY", sector="Energy", min_amount_outstanding=500)
    """
    from datetime import datetime
    from dateutil.relativedelta import relativedelta

    conn = get_wrds_connection()
    latest_month = _detect_latest_full_month(conn)
    latest_dt = datetime.strptime(latest_month, "%Y-%m-%d")

    params: dict = {
        "latest_month": latest_month,
        "date_3mo": (latest_dt - relativedelta(months=3)).strftime("%Y-%m-%d"),
    }
    filters: list[str] = []
    filters_applied: dict = {}

    # --- Filters ---
    if ticker is not None:
        ticker = validate_ticker(ticker)
        params["ticker"] = ticker
        filters.append("UPPER(b.company_symbol) = :ticker")
        filters_applied["ticker"] = ticker

    if rating_class is not None:
        rc = rating_class.strip().upper()
        if rc not in ("HY", "IG"):
            raise ToolError("rating_class must be 'HY' or 'IG'.")
        params["rating_class"] = f"{'1' if rc == 'HY' else '0'}.{rc}"
        filters.append("b.rating_class = :rating_class")
        filters_applied["rating_class"] = rc

    if min_rating is not None:
        params["min_rating_num"] = _validate_rating(min_rating)
        filters.append(f"({RATING_CAT_TO_NUM_SQL}) >= :min_rating_num")
        filters_applied["min_rating"] = min_rating

    if max_rating is not None:
        params["max_rating_num"] = _validate_rating(max_rating)
        filters.append(f"({RATING_CAT_TO_NUM_SQL}) <= :max_rating_num")
        filters_applied["max_rating"] = max_rating

    if security_level is not None:
        sl = security_level.strip().upper()
        params["security_level"] = sl
        filters.append("fi.security_level = :security_level")
        filters_applied["security_level"] = sl

    if min_amount_outstanding is not None:
        params["min_amt"] = min_amount_outstanding
        filters.append("b.amount_outstanding >= :min_amt")
        filters_applied["min_amount_outstanding"] = min_amount_outstanding

    if min_coupon is not None:
        params["min_coupon"] = min_coupon
        filters.append("fi.coupon >= :min_coupon")
        filters_applied["min_coupon"] = min_coupon

    if max_coupon is not None:
        params["max_coupon"] = max_coupon
        filters.append("fi.coupon <= :max_coupon")
        filters_applied["max_coupon"] = max_coupon

    if maturity_after is not None:
        validate_date(maturity_after, "maturity_after")
        params["maturity_after"] = maturity_after
        filters.append("fi.maturity > :maturity_after")
        filters_applied["maturity_after"] = maturity_after

    if maturity_before is not None:
        validate_date(maturity_before, "maturity_before")
        params["maturity_before"] = maturity_before
        filters.append("fi.maturity < :maturity_before")
        filters_applied["maturity_before"] = maturity_before

    if min_spread is not None:
        params["min_spread"] = min_spread
        filters.append("b.t_spread >= :min_spread")
        filters_applied["min_spread"] = min_spread

    if max_spread is not None:
        params["max_spread"] = max_spread
        filters.append("b.t_spread <= :max_spread")
        filters_applied["max_spread"] = max_spread

    # Sector filter requires Compustat join
    need_sector_join = sector is not None
    if need_sector_join:
        sector_clean = sector.strip().title()
        if sector_clean not in SECTOR_SIC_RANGES:
            valid = ", ".join(sorted(SECTOR_SIC_RANGES.keys()))
            raise ToolError(f"Unknown sector '{sector}'. Valid sectors: {valid}")
        sic_clause = _build_sic_filter(SECTOR_SIC_RANGES[sector_clean], params, col="funda.sich")
        filters.append(sic_clause)
        filters_applied["sector"] = sector_clean

    where_clause = " AND ".join(filters) if filters else "TRUE"

    # Sort
    valid_sorts = {"spread", "yield", "amount_outstanding", "maturity", "coupon", "rating"}
    if sort_by not in valid_sorts:
        sort_by = "spread"
    sort_col = {
        "spread": "b.t_spread DESC NULLS LAST",
        "yield": "b.yield DESC NULLS LAST",
        "amount_outstanding": "b.amount_outstanding DESC NULLS LAST",
        "maturity": "fi.maturity ASC NULLS LAST",
        "coupon": "fi.coupon DESC NULLS LAST",
        "rating": f"({RATING_CAT_TO_NUM_SQL}) ASC NULLS LAST",
    }[sort_by]

    # Sector join clause
    sector_join = ""
    if need_sector_join:
        sector_join = """
        INNER JOIN comp.security sec
            ON UPPER(sec.tic) = UPPER(b.company_symbol)
        INNER JOIN comp.funda funda
            ON sec.gvkey = funda.gvkey
            AND funda.indfmt = 'INDL' AND funda.datafmt = 'STD' AND funda.consol = 'C'
            AND funda.datadate = (SELECT MAX(f2.datadate) FROM comp.funda f2
                                  WHERE f2.gvkey = funda.gvkey
                                    AND f2.indfmt = 'INDL' AND f2.datafmt = 'STD' AND f2.consol = 'C')
        """

    params["limit"] = limit

    query = f"""
        WITH bond_returns AS (
            SELECT cusip,
                   EXP(SUM(LN(1 + ret_eom))) - 1 AS ret_3mo
            FROM wrdsapps_bondret.bondret
            WHERE date >= :date_3mo ::date
              AND date <= :latest_month ::date
              AND ret_eom IS NOT NULL
              AND ret_eom > -1
            GROUP BY cusip
        )
        SELECT
            b.cusip,
            b.company_symbol AS ticker,
            fi.coupon,
            fi.maturity::text AS maturity,
            fi.security_level,
            fi.offering_amt,
            b.amount_outstanding,
            b.r_sp AS sp_rating,
            b.r_mr AS moody_rating,
            b.rating_class,
            b.rating_cat,
            ROUND(b.t_spread::numeric, 1) AS spread_bps,
            ROUND(b.yield::numeric, 3) AS yield,
            ROUND(b.price_eom::numeric, 3) AS price,
            ROUND(b.duration::numeric, 2) AS duration,
            ROUND(b.ret_eom::numeric, 4) AS return_1mo,
            ROUND(br.ret_3mo::numeric, 4) AS return_3mo
        FROM wrdsapps_bondret.bondret b
        INNER JOIN fisd.fisd_mergedissue fi
            ON b.cusip = fi.complete_cusip
        LEFT JOIN bond_returns br
            ON b.cusip = br.cusip
        {sector_join}
        WHERE b.date = :latest_month
          AND b.company_symbol IS NOT NULL
          AND fi.asset_backed = 'N'
          AND fi.convertible = 'N'
          AND {where_clause}
        ORDER BY {sort_col}
        LIMIT :limit
    """

    logger.info("screen_bonds: filters=%s", filters_applied)

    try:
        df = conn.raw_sql(query, params=params)
    except Exception as e:
        raise ToolError(f"Bond screening query failed: {e}")

    if df.empty:
        return {
            "as_of_date": latest_month,
            "filters_applied": filters_applied,
            "result_count": 0,
            "bonds": [{"message": "No bonds match the specified criteria."}],
        }

    records = df_to_records(df)

    return {
        "as_of_date": latest_month,
        "filters_applied": filters_applied,
        "result_count": len(records),
        "bonds": records,
    }


# --- Tool 3: get_market_benchmarks ---

@screening_mcp.tool
def get_market_benchmarks(
    start_date: Annotated[str, Field(description="Start date YYYY-MM-DD")],
    end_date: Annotated[str, Field(description="End date YYYY-MM-DD")],
    rating_class: Annotated[str | None, Field(description="'HY' or 'IG' (default: both)")] = None,
    rating_category: Annotated[str | None, Field(description="Specific rating bucket: 'AAA','AA','A','BBB','BB','B','CCC'")] = None,
    ctx: Context = None,
) -> dict:
    """Get aggregate bond market benchmarks over time — monthly index-style returns.

    Computes equal-weighted and value-weighted average spread, yield, return,
    and duration for the bond universe, broken down by month. Can be filtered
    to IG, HY, or a specific rating category.

    Useful for comparing an issuer's spread/return vs the market.

    Returns: dict with filters, monthly_data (list of monthly aggregates),
    and summary (period totals).

    Example: get_market_benchmarks("2024-01-01", "2025-03-31", rating_class="HY")
    """
    start_date, end_date = validate_date_range(start_date, end_date)

    # Validate before connecting
    params: dict = {"start_date": start_date, "end_date": end_date}
    filters: list[str] = []
    filters_applied: dict = {}

    if rating_class is not None:
        rc = rating_class.strip().upper()
        if rc not in ("HY", "IG"):
            raise ToolError("rating_class must be 'HY' or 'IG'.")
        params["rating_class"] = f"{'1' if rc == 'HY' else '0'}.{rc}"
        filters.append("rating_class = :rating_class")
        filters_applied["rating_class"] = rc

    if rating_category is not None:
        cat = rating_category.strip().upper()
        valid_cats = ["AAA", "AA", "A", "BBB", "BB", "B", "CCC", "CC", "C", "D"]
        if cat not in valid_cats:
            raise ToolError(f"Invalid rating_category '{cat}'. Valid: {', '.join(valid_cats)}")
        params["rating_cat"] = cat
        filters.append("rating_cat = :rating_cat")
        filters_applied["rating_category"] = cat

    conn = get_wrds_connection()

    where_extra = (" AND " + " AND ".join(filters)) if filters else ""

    query = f"""
        SELECT
            date,
            COUNT(*) AS bond_count,
            COUNT(DISTINCT company_symbol) AS issuer_count,
            ROUND(AVG(t_spread)::numeric, 1) AS avg_spread,
            ROUND(AVG(yield)::numeric, 3) AS avg_yield,
            ROUND(AVG(ret_eom)::numeric, 6) AS avg_return,
            ROUND(AVG(duration)::numeric, 2) AS avg_duration,
            ROUND(AVG(price_eom)::numeric, 2) AS avg_price,
            ROUND(SUM(amount_outstanding)::numeric, 0) AS total_outstanding,
            ROUND(
                SUM(t_spread * amount_outstanding) / NULLIF(SUM(amount_outstanding), 0)
            ::numeric, 1) AS vw_spread,
            ROUND(
                SUM(yield * amount_outstanding) / NULLIF(SUM(amount_outstanding), 0)
            ::numeric, 3) AS vw_yield,
            ROUND(
                SUM(ret_eom * amount_outstanding) / NULLIF(SUM(amount_outstanding), 0)
            ::numeric, 6) AS vw_return
        FROM wrdsapps_bondret.bondret
        WHERE date BETWEEN :start_date AND :end_date
          AND company_symbol IS NOT NULL
          AND t_spread IS NOT NULL
          AND ret_eom IS NOT NULL
          AND ret_eom > -1
          {where_extra}
        GROUP BY date
        ORDER BY date
    """

    try:
        df = conn.raw_sql(query, params=params)
    except Exception as e:
        raise ToolError(f"Benchmark query failed: {e}")

    if df.empty:
        return {
            "filters_applied": filters_applied,
            "monthly_data": [{"message": "No benchmark data for the specified period."}],
        }

    records = df_to_records(df)

    # Compute cumulative return
    cum_return = 1.0
    for r in records:
        if r.get("vw_return") is not None:
            cum_return *= (1 + r["vw_return"])
            r["cumulative_vw_return"] = round(cum_return - 1, 6)

    return {
        "filters_applied": filters_applied,
        "period": {"start": start_date, "end": end_date},
        "months": len(records),
        "summary": {
            "avg_spread_bps": round(sum(r.get("vw_spread", 0) or 0 for r in records) / len(records), 1) if records else None,
            "cumulative_vw_return": round(cum_return - 1, 4),
            "total_months": len(records),
        },
        "monthly_data": records,
    }


# --- Tool 4: get_relative_value ---

@screening_mcp.tool
def get_relative_value(
    ticker: Annotated[str, Field(description="Company ticker symbol")],
    ctx: Context = None,
) -> dict:
    """Compare an issuer's bonds to rating-category and sector peers.

    For the given issuer, shows each bond's spread, yield, and return vs
    the average for bonds in the same rating category and (if available)
    same sector. Helps identify if bonds are cheap or rich relative to peers.

    Returns: dict with issuer bonds, peer_stats (rating-based averages),
    and per-bond relative value metrics (spread_vs_peers, yield_vs_peers).

    Example: get_relative_value("F")
    """
    ticker = validate_ticker(ticker)
    conn = get_wrds_connection()
    latest_month = _detect_latest_full_month(conn)

    # Get issuer's bonds and their rating category
    try:
        issuer_df = conn.raw_sql("""
            SELECT
                b.cusip,
                b.company_symbol AS ticker,
                fi.coupon,
                fi.maturity::text AS maturity,
                fi.security_level,
                b.amount_outstanding,
                b.r_sp AS sp_rating,
                b.r_mr AS moody_rating,
                b.rating_cat,
                b.rating_class,
                ROUND(b.t_spread::numeric, 1) AS spread_bps,
                ROUND(b.yield::numeric, 3) AS yield,
                ROUND(b.price_eom::numeric, 3) AS price,
                ROUND(b.duration::numeric, 2) AS duration,
                ROUND(b.ret_eom::numeric, 4) AS return_1mo
            FROM wrdsapps_bondret.bondret b
            INNER JOIN fisd.fisd_mergedissue fi
                ON b.cusip = fi.complete_cusip
            WHERE b.date = :latest_month
              AND UPPER(b.company_symbol) = :ticker
              AND fi.asset_backed = 'N'
              AND fi.convertible = 'N'
            ORDER BY b.t_spread DESC NULLS LAST
        """, params={"latest_month": latest_month, "ticker": ticker})
    except Exception as e:
        raise ToolError(f"Issuer query failed: {e}")

    if issuer_df.empty:
        return {"ticker": ticker, "message": f"No bonds found for {ticker} in latest bondret month."}

    # Determine rating categories present
    rating_cats = issuer_df["rating_cat"].dropna().unique().tolist()
    rating_class_val = issuer_df["rating_class"].dropna().iloc[0] if not issuer_df["rating_class"].dropna().empty else None

    # Get peer stats for each rating category
    peer_stats = {}
    if rating_cats:
        cat_list = ", ".join(f"'{c}'" for c in rating_cats)
        try:
            peer_df = conn.raw_sql(f"""
                SELECT
                    b.rating_cat,
                    COUNT(*) AS bond_count,
                    COUNT(DISTINCT b.company_symbol) AS issuer_count,
                    ROUND(AVG(b.t_spread)::numeric, 1) AS avg_spread,
                    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY b.t_spread)::numeric, 1) AS spread_p25,
                    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY b.t_spread)::numeric, 1) AS spread_median,
                    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY b.t_spread)::numeric, 1) AS spread_p75,
                    ROUND(AVG(b.yield)::numeric, 3) AS avg_yield,
                    ROUND(AVG(b.duration)::numeric, 2) AS avg_duration,
                    ROUND(AVG(b.price_eom)::numeric, 2) AS avg_price,
                    ROUND(AVG(b.ret_eom)::numeric, 4) AS avg_return_1mo
                FROM wrdsapps_bondret.bondret b
                INNER JOIN fisd.fisd_mergedissue fi
                    ON b.cusip = fi.complete_cusip
                WHERE b.date = :latest_month
                  AND b.rating_cat IN ({cat_list})
                  AND b.company_symbol IS NOT NULL
                  AND UPPER(b.company_symbol) != :ticker
                  AND b.t_spread IS NOT NULL
                  AND fi.asset_backed = 'N'
                  AND fi.convertible = 'N'
                GROUP BY b.rating_cat
            """, params={"latest_month": latest_month, "ticker": ticker})

            for _, row in peer_df.iterrows():
                cat = row["rating_cat"]
                peer_stats[cat] = {
                    "bond_count": int(row["bond_count"]),
                    "issuer_count": int(row["issuer_count"]),
                    "avg_spread": float(row["avg_spread"]) if pd.notna(row.get("avg_spread")) else None,
                    "spread_p25": float(row["spread_p25"]) if pd.notna(row.get("spread_p25")) else None,
                    "spread_median": float(row["spread_median"]) if pd.notna(row.get("spread_median")) else None,
                    "spread_p75": float(row["spread_p75"]) if pd.notna(row.get("spread_p75")) else None,
                    "avg_yield": float(row["avg_yield"]) if pd.notna(row.get("avg_yield")) else None,
                    "avg_duration": float(row["avg_duration"]) if pd.notna(row.get("avg_duration")) else None,
                    "avg_price": float(row["avg_price"]) if pd.notna(row.get("avg_price")) else None,
                    "avg_return_1mo": float(row["avg_return_1mo"]) if pd.notna(row.get("avg_return_1mo")) else None,
                }
        except Exception as e:
            logger.warning("Peer stats query failed: %s", e)

    # Build per-bond relative value
    bonds = []
    for _, row in issuer_df.iterrows():
        bond: dict = {}
        for col in issuer_df.columns:
            val = row[col]
            if pd.notna(val):
                bond[col] = float(val) if isinstance(val, (int, float)) else str(val)

        cat = row.get("rating_cat")
        if cat and cat in peer_stats:
            peers = peer_stats[cat]
            if bond.get("spread_bps") is not None and peers.get("avg_spread") is not None:
                bond["spread_vs_peers"] = round(bond["spread_bps"] - peers["avg_spread"], 1)
                bond["spread_vs_median"] = round(bond["spread_bps"] - peers["spread_median"], 1) if peers.get("spread_median") else None
                # Positive = wider than peers (cheap), negative = tighter (rich)
                bond["relative_value"] = "cheap" if bond["spread_vs_peers"] > 20 else (
                    "rich" if bond["spread_vs_peers"] < -20 else "fair"
                )
            if bond.get("yield") is not None and peers.get("avg_yield") is not None:
                bond["yield_vs_peers"] = round(float(bond["yield"]) - peers["avg_yield"], 3)

        bonds.append(bond)

    return {
        "ticker": ticker,
        "as_of_date": latest_month,
        "rating_class": rating_class_val,
        "bond_count": len(bonds),
        "peer_stats": peer_stats,
        "bonds": bonds,
    }
