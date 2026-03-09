"""Credit ratings tools for WRDS MCP."""

import logging
from typing import Annotated

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


@ratings_mcp.tool
def get_credit_ratings(
    ticker: Annotated[str, Field(description="Company ticker symbol, e.g. 'AAPL'")],
    ctx: Context = None,
) -> dict:
    """Get the current/most recent S&P credit rating for a company.

    Queries comp.adsprate for the latest S&P long-term issuer credit rating
    (splticrm) and subordinated debt rating.

    Returns: dict with ticker, gvkey, rating, rating_date, subordinated_rating,
    short_term_rating.

    Note: S&P ratings via Compustat cover through Feb 2017.

    Example: get_credit_ratings("AAPL")
    """
    ticker = validate_ticker(ticker)
    conn = get_wrds_connection()

    gvkey = resolve_ticker_to_gvkey(conn, ticker)
    if gvkey is None:
        raise ToolError(f"Ticker '{ticker}' not found in Compustat.")

    query = """
        SELECT gvkey, datadate, splticrm, spsdrm, spsticrm
        FROM comp.adsprate
        WHERE gvkey = %(gvkey)s
          AND splticrm IS NOT NULL
        ORDER BY datadate DESC
        LIMIT 1
    """

    logger.debug("get_credit_ratings: ticker=%s, gvkey=%s", ticker, gvkey)

    try:
        df = conn.raw_sql(query, params={"gvkey": gvkey}, date_cols=["datadate"])
    except Exception as e:
        raise ToolError(f"WRDS query failed: {e}")

    if df.empty:
        return {
            "ticker": ticker,
            "gvkey": gvkey,
            "rating": None,
            "rating_date": None,
            "message": "No S&P ratings found. Note: Compustat ratings data covers through Feb 2017.",
        }

    row = df.iloc[0]
    return {
        "ticker": ticker,
        "gvkey": gvkey,
        "rating": row.get("splticrm"),
        "rating_date": row["datadate"].isoformat()[:10] if hasattr(row["datadate"], "isoformat") else str(row["datadate"]),
        "subordinated_rating": row.get("spsdrm") if not _is_na(row.get("spsdrm")) else None,
        "short_term_rating": row.get("spsticrm") if not _is_na(row.get("spsticrm")) else None,
        "note": "S&P ratings via Compustat cover through Feb 2017.",
    }


@ratings_mcp.tool
def get_ratings_history(
    ticker: Annotated[str, Field(description="Company ticker symbol")],
    start_date: Annotated[str, Field(description="Start date in YYYY-MM-DD format")],
    end_date: Annotated[str, Field(description="End date in YYYY-MM-DD format")],
    ctx: Context = None,
) -> list[dict]:
    """Get S&P credit rating changes over time for a company.

    Queries comp.adsprate for all rating observations in the date range.
    Identifies rating transitions (upgrades/downgrades).

    Returns: list of dicts with date, rating, previous_rating, direction.

    Example: get_ratings_history("AAPL", "2010-01-01", "2017-01-01")
    """
    ticker = validate_ticker(ticker)
    start_date, end_date = validate_date_range(start_date, end_date)

    conn = get_wrds_connection()

    gvkey = resolve_ticker_to_gvkey(conn, ticker)
    if gvkey is None:
        raise ToolError(f"Ticker '{ticker}' not found in Compustat.")

    query = """
        SELECT gvkey, datadate, splticrm
        FROM comp.adsprate
        WHERE gvkey = %(gvkey)s
          AND datadate BETWEEN %(start_date)s AND %(end_date)s
          AND splticrm IS NOT NULL
        ORDER BY datadate
    """

    logger.debug(
        "get_ratings_history: ticker=%s, gvkey=%s, start=%s, end=%s",
        ticker, gvkey, start_date, end_date,
    )

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
            "note": "S&P ratings via Compustat cover through Feb 2017.",
        }]

    results = []
    previous_rating = None
    for _, row in df.iterrows():
        current_rating = row["splticrm"]
        date_str = row["datadate"].isoformat()[:10] if hasattr(row["datadate"], "isoformat") else str(row["datadate"])
        direction = _rating_direction(previous_rating, current_rating)

        results.append({
            "date": date_str,
            "rating": current_rating,
            "previous_rating": previous_rating,
            "direction": direction,
        })
        previous_rating = current_rating

    return results


def _is_na(value) -> bool:
    """Check if a value is NaN/None/NaT."""
    if value is None:
        return True
    try:
        import pandas as pd
        return pd.isna(value)
    except (TypeError, ValueError):
        return False
