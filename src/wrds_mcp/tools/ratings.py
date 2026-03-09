"""Credit ratings tools for WRDS MCP."""

import logging
from typing import Annotated

from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field

from wrds_mcp.tools._validation import validate_date, validate_date_range, validate_ticker

logger = logging.getLogger(__name__)

ratings_mcp = FastMCP("Ratings")


@ratings_mcp.tool
def get_credit_ratings(
    ticker: Annotated[str, Field(description="Company ticker symbol, e.g. 'AAPL'")],
    ctx: Context = None,
) -> dict:
    """Get the current/most recent S&P credit rating for a company.

    Queries comp.adsprate for the latest S&P long-term issuer credit rating
    (splticrm) and subordinated debt rating.

    Returns: dict with ticker, gvkey, rating, rating_date, subordinated_rating.

    Note: S&P ratings via Compustat cover through Feb 2017.

    Example: get_credit_ratings("AAPL")
    """
    raise NotImplementedError("Will be implemented in Phase 3")


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
    raise NotImplementedError("Will be implemented in Phase 3")
