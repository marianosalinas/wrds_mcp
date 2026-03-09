"""Bond and TRACE transaction tools for WRDS MCP."""

import logging
from typing import Annotated

from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field

from wrds_mcp.tools._validation import validate_date, validate_date_range, validate_ticker, validate_cusip

logger = logging.getLogger(__name__)

bonds_mcp = FastMCP("Bonds")


@bonds_mcp.tool
def get_bond_transactions(
    ticker: Annotated[str, Field(description="Company ticker symbol, e.g. 'AAPL'")],
    start_date: Annotated[str, Field(description="Start date in YYYY-MM-DD format")],
    end_date: Annotated[str, Field(description="End date in YYYY-MM-DD format")],
    ctx: Context = None,
) -> list[dict]:
    """Get TRACE transaction-level bond data for a company.

    Queries trace.trace_enhanced joined with fisd.fisd_mergedissue to find
    all bond transactions for the given ticker in the date range.

    Returns: list of dicts with cusip, trade_date, price, yield, volume, buy_sell.

    Example: get_bond_transactions("AAPL", "2024-01-01", "2024-06-30")
    """
    raise NotImplementedError("Will be implemented in Phase 3")


@bonds_mcp.tool
def get_bond_yield_history(
    cusip: Annotated[str, Field(description="9-character CUSIP identifier")],
    start_date: Annotated[str, Field(description="Start date in YYYY-MM-DD format")],
    end_date: Annotated[str, Field(description="End date in YYYY-MM-DD format")],
    ctx: Context = None,
) -> list[dict]:
    """Get yield time series for a specific bond by CUSIP.

    Queries trace.trace_enhanced for the given CUSIP, aggregating daily
    volume-weighted average yield.

    Returns: list of dicts with date, avg_yield, avg_price, total_volume, num_trades.

    Example: get_bond_yield_history("037833100", "2024-01-01", "2024-12-31")
    """
    raise NotImplementedError("Will be implemented in Phase 3")


@bonds_mcp.tool
def get_company_bonds(
    ticker: Annotated[str, Field(description="Company ticker symbol")],
    ctx: Context = None,
) -> list[dict]:
    """Get all outstanding bonds for a company.

    Queries fisd.fisd_mergedissue joined with fisd.fisd_mergedissuer
    for corporate bonds matching the ticker.

    Returns: list of dicts with cusip, coupon, maturity, offering_amount,
    security_level, bond_type, coupon_type.

    Example: get_company_bonds("AAPL")
    """
    raise NotImplementedError("Will be implemented in Phase 3")
