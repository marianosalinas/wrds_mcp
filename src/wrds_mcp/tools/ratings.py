"""Credit ratings tools for WRDS MCP.

Primary source: wrdsapps_bondret.bondret (S&P, Moody's, Fitch through Dec 2025).
Fallback: comp.adsprate (S&P only, through Feb 2017).
"""

import logging
from typing import Annotated

import pandas as pd
from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field

from wrds_mcp.db.connection import get_wrds_connection, resolve_ticker_to_gvkey
from wrds_mcp.tools._validation import (
    df_to_records,
    validate_date_range,
    validate_ticker,
)

logger = logging.getLogger(__name__)

ratings_mcp = FastMCP("Ratings")

# S&P rating scale for determining upgrade/downgrade direction
SP_RATING_SCALE = {
    "AAA": 1, "AA+": 2, "AA": 3, "AA-": 4,
    "A+": 5, "A": 6, "A-": 7,
    "BBB+": 8, "BBB": 9, "BBB-": 10,
    "BB+": 11, "BB": 12, "BB-": 13,
    "B+": 14, "B": 15, "B-": 16,
    "CCC+": 17, "CCC": 18, "CCC-": 19,
    "CC": 20, "C": 21, "D": 22, "SD": 23,
}


def _rating_direction(previous: str | None, current: str | None) -> str:
    """Determine if a rating change is an upgrade, downgrade, or affirmation."""
    if previous is None or current is None:
        return "initial" if previous is None else "withdrawn"
    prev_rank = SP_RATING_SCALE.get(previous.strip())
    curr_rank = SP_RATING_SCALE.get(current.strip())
    if prev_rank is None or curr_rank is None:
        return "unknown"
    if curr_rank < prev_rank:
        return "upgrade"
    elif curr_rank > prev_rank:
        return "downgrade"
    return "affirmed"


def _is_na(value) -> bool:
    """Check if a value is NaN/None/NaT."""
    if value is None:
        return True
    try:
        return pd.isna(value)
    except (TypeError, ValueError):
        return False


@ratings_mcp.tool
def get_credit_ratings(
    ticker: Annotated[str, Field(description="Company ticker symbol, e.g. 'AAPL'")],
    ctx: Context = None,
) -> dict:
    """Get the current credit ratings for a company from S&P, Moody's, and Fitch.

    Primary source: WRDS Bond Returns database (wrdsapps_bondret.bondret),
    which has multi-agency ratings current through the latest available month.
    Falls back to Compustat (comp.adsprate) for S&P-only historical ratings.

    Returns: dict with ticker, sp_rating, moody_rating, fitch_rating,
    as_of_date, rating_category, and source.

    Example: get_credit_ratings("F")
    """
    ticker = validate_ticker(ticker)
    conn = get_wrds_connection()

    # Primary: bondret — has S&P, Moody's, Fitch, current through latest month
    query = """
        SELECT DISTINCT ON (company_symbol)
               company_symbol, date,
               r_sp, r_mr, r_fr,
               n_sp, n_mr, n_fr,
               rating_num, rating_cat, rating_class
        FROM wrdsapps_bondret.bondret
        WHERE UPPER(company_symbol) = :ticker
          AND (r_sp IS NOT NULL OR r_mr IS NOT NULL OR r_fr IS NOT NULL)
        ORDER BY company_symbol, date DESC
    """

    logger.debug("get_credit_ratings: ticker=%s (bondret)", ticker)

    try:
        df = conn.raw_sql(query, params={"ticker": ticker}, date_cols=["date"])
    except Exception as e:
        logger.warning("bondret query failed, falling back to Compustat: %s", e)
        df = pd.DataFrame()

    if not df.empty:
        row = df.iloc[0]
        return {
            "ticker": ticker,
            "as_of_date": row["date"].isoformat()[:10] if hasattr(row["date"], "isoformat") else str(row["date"]),
            "sp_rating": row["r_sp"] if not _is_na(row.get("r_sp")) else None,
            "moody_rating": row["r_mr"] if not _is_na(row.get("r_mr")) else None,
            "fitch_rating": row["r_fr"] if not _is_na(row.get("r_fr")) else None,
            "sp_numeric": int(row["n_sp"]) if not _is_na(row.get("n_sp")) else None,
            "moody_numeric": int(row["n_mr"]) if not _is_na(row.get("n_mr")) else None,
            "fitch_numeric": int(row["n_fr"]) if not _is_na(row.get("n_fr")) else None,
            "composite_rating_num": float(row["rating_num"]) if not _is_na(row.get("rating_num")) else None,
            "rating_category": row["rating_cat"] if not _is_na(row.get("rating_cat")) else None,
            "rating_class": row["rating_class"] if not _is_na(row.get("rating_class")) else None,
            "source": "wrdsapps_bondret.bondret",
        }

    # Fallback: Compustat adsprate (S&P only, through Feb 2017)
    gvkey = resolve_ticker_to_gvkey(conn, ticker)
    if gvkey is None:
        raise ToolError(f"Ticker '{ticker}' not found in WRDS Bond Returns or Compustat.")

    query_cstat = """
        SELECT gvkey, datadate, splticrm, spsdrm, spsticrm
        FROM comp.adsprate
        WHERE gvkey = :gvkey
          AND splticrm IS NOT NULL
        ORDER BY datadate DESC
        LIMIT 1
    """

    logger.debug("get_credit_ratings: ticker=%s, gvkey=%s (Compustat fallback)", ticker, gvkey)

    try:
        df = conn.raw_sql(query_cstat, params={"gvkey": gvkey}, date_cols=["datadate"])
    except Exception as e:
        raise ToolError(f"WRDS query failed: {e}")

    if df.empty:
        return {
            "ticker": ticker,
            "sp_rating": None,
            "moody_rating": None,
            "fitch_rating": None,
            "message": "No ratings found in Bond Returns or Compustat.",
        }

    row = df.iloc[0]
    return {
        "ticker": ticker,
        "as_of_date": row["datadate"].isoformat()[:10] if hasattr(row["datadate"], "isoformat") else str(row["datadate"]),
        "sp_rating": row.get("splticrm"),
        "moody_rating": None,
        "fitch_rating": None,
        "note": "S&P only (Compustat fallback). Compustat ratings end Feb 2017.",
        "source": "comp.adsprate",
    }


@ratings_mcp.tool
def get_ratings_history(
    ticker: Annotated[str, Field(description="Company ticker symbol")],
    start_date: Annotated[str, Field(description="Start date in YYYY-MM-DD format")],
    end_date: Annotated[str, Field(description="End date in YYYY-MM-DD format")],
    ctx: Context = None,
) -> list[dict]:
    """Get credit rating changes over time for a company.

    Uses the WRDS Bond Returns database for monthly multi-agency ratings
    (S&P, Moody's, Fitch). Only includes months where a rating changed.

    Returns: list of dicts with date, sp_rating, moody_rating, fitch_rating,
    change_description.

    Example: get_ratings_history("F", "2020-01-01", "2025-12-31")
    """
    ticker = validate_ticker(ticker)
    start_date, end_date = validate_date_range(start_date, end_date)

    conn = get_wrds_connection()

    # Use bondret for multi-agency ratings
    query = """
        SELECT DISTINCT date, r_sp, r_mr, r_fr, rating_cat, rating_class
        FROM wrdsapps_bondret.bondret
        WHERE UPPER(company_symbol) = :ticker
          AND date BETWEEN :start_date AND :end_date
          AND (r_sp IS NOT NULL OR r_mr IS NOT NULL OR r_fr IS NOT NULL)
        ORDER BY date
    """

    logger.debug(
        "get_ratings_history: ticker=%s, start=%s, end=%s",
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
        # Fallback to Compustat for older dates
        return _ratings_history_compustat(conn, ticker, start_date, end_date)

    # Deduplicate by month (bondret has per-bond rows, we want issuer-level)
    df = df.drop_duplicates(subset=["date", "r_sp", "r_mr", "r_fr"]).sort_values("date")

    # Track changes — only emit rows where a rating actually changed
    results = []
    prev_sp = None
    prev_mr = None
    prev_fr = None

    for _, row in df.iterrows():
        sp = row["r_sp"] if not _is_na(row.get("r_sp")) else None
        mr = row["r_mr"] if not _is_na(row.get("r_mr")) else None
        fr = row["r_fr"] if not _is_na(row.get("r_fr")) else None
        date_str = row["date"].isoformat()[:10] if hasattr(row["date"], "isoformat") else str(row["date"])

        if sp == prev_sp and mr == prev_mr and fr == prev_fr:
            continue  # No change this month

        changes = []
        if sp != prev_sp:
            direction = _rating_direction(prev_sp, sp)
            changes.append(f"S&P: {prev_sp or 'N/A'} → {sp or 'N/A'} ({direction})")
        if mr != prev_mr:
            changes.append(f"Moody's: {prev_mr or 'N/A'} → {mr or 'N/A'}")
        if fr != prev_fr:
            changes.append(f"Fitch: {prev_fr or 'N/A'} → {fr or 'N/A'}")

        results.append({
            "date": date_str,
            "sp_rating": sp,
            "moody_rating": mr,
            "fitch_rating": fr,
            "rating_category": row["rating_cat"] if not _is_na(row.get("rating_cat")) else None,
            "changes": "; ".join(changes) if changes else "initial",
        })

        prev_sp, prev_mr, prev_fr = sp, mr, fr

    if not results:
        return [{"message": f"No rating changes for {ticker} between {start_date} and {end_date}."}]

    return results


def _ratings_history_compustat(conn, ticker: str, start_date: str, end_date: str) -> list[dict]:
    """Fallback: get ratings history from Compustat (S&P only, through Feb 2017)."""
    gvkey = resolve_ticker_to_gvkey(conn, ticker)
    if gvkey is None:
        return [{"message": f"No rating history for {ticker}. Ticker not found in Bond Returns or Compustat."}]

    query = """
        SELECT gvkey, datadate, splticrm
        FROM comp.adsprate
        WHERE gvkey = :gvkey
          AND datadate BETWEEN :start_date AND :end_date
          AND splticrm IS NOT NULL
        ORDER BY datadate
    """

    try:
        df = conn.raw_sql(
            query,
            params={"gvkey": gvkey, "start_date": start_date, "end_date": end_date},
            date_cols=["datadate"],
        )
    except Exception as e:
        raise ToolError(f"WRDS query failed: {e}")

    if df.empty:
        return [{
            "message": f"No rating history for {ticker} between {start_date} and {end_date}.",
            "note": "Checked both Bond Returns and Compustat (S&P through Feb 2017).",
        }]

    results = []
    previous_rating = None
    for _, row in df.iterrows():
        current_rating = row["splticrm"]
        if current_rating == previous_rating:
            continue
        date_str = row["datadate"].isoformat()[:10] if hasattr(row["datadate"], "isoformat") else str(row["datadate"])
        direction = _rating_direction(previous_rating, current_rating)

        results.append({
            "date": date_str,
            "sp_rating": current_rating,
            "moody_rating": None,
            "fitch_rating": None,
            "changes": f"S&P: {previous_rating or 'N/A'} → {current_rating} ({direction})",
            "source": "comp.adsprate (S&P only, through Feb 2017)",
        })
        previous_rating = current_rating

    return results if results else [{"message": f"No rating changes for {ticker} between {start_date} and {end_date}."}]
