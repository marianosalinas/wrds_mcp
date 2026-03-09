"""Financial metrics tools for WRDS MCP (leverage, coverage, liquidity)."""

import logging
from typing import Annotated

from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field

from wrds_mcp.tools._validation import validate_ticker

logger = logging.getLogger(__name__)

financials_mcp = FastMCP("Financials")


@financials_mcp.tool
def get_leverage_metrics(
    ticker: Annotated[str, Field(description="Company ticker symbol, e.g. 'AAPL'")],
    periods: Annotated[int, Field(description="Number of annual periods", ge=1, le=20)] = 5,
    ctx: Context = None,
) -> list[dict]:
    """Get leverage metrics from Compustat annual fundamentals.

    Computes total_debt (dltt+dlc), EBITDA (oibdp), debt_to_ebitda,
    and net_debt_to_ebitda (using che for cash).

    Returns: list of dicts with fiscal_year, datadate, total_debt, ebitda,
    debt_to_ebitda, net_debt, net_debt_to_ebitda, total_assets.

    Example: get_leverage_metrics("AAPL", periods=5)
    """
    raise NotImplementedError("Will be implemented in Phase 3")


@financials_mcp.tool
def get_coverage_ratios(
    ticker: Annotated[str, Field(description="Company ticker symbol")],
    periods: Annotated[int, Field(description="Number of annual periods", ge=1, le=20)] = 5,
    ctx: Context = None,
) -> list[dict]:
    """Get interest coverage and fixed charge coverage ratios.

    Interest coverage = oibdp / xint.
    Fixed charge coverage = (oibdp + xrent) / (xint + xrent).

    Returns: list of dicts with fiscal_year, datadate, ebitda,
    interest_expense, interest_coverage, fixed_charge_coverage.

    Example: get_coverage_ratios("AAPL", periods=5)
    """
    raise NotImplementedError("Will be implemented in Phase 3")


@financials_mcp.tool
def get_liquidity_metrics(
    ticker: Annotated[str, Field(description="Company ticker symbol")],
    periods: Annotated[int, Field(description="Number of annual periods", ge=1, le=20)] = 5,
    ctx: Context = None,
) -> list[dict]:
    """Get liquidity metrics for a company.

    Queries comp.funda for current ratio (act/lct), cash & equivalents (che),
    and short-term investments (ivst).

    Returns: list of dicts with fiscal_year, datadate, current_ratio,
    cash_and_equivalents, short_term_investments, current_assets,
    current_liabilities.

    Example: get_liquidity_metrics("AAPL", periods=3)
    """
    raise NotImplementedError("Will be implemented in Phase 3")


@financials_mcp.tool
def get_credit_summary(
    ticker: Annotated[str, Field(description="Company ticker symbol")],
    ctx: Context = None,
) -> dict:
    """Get a combined credit snapshot: leverage, coverage, ratings, and bonds.

    Combines get_leverage_metrics (1 period), get_coverage_ratios (1 period),
    get_credit_ratings, and get_company_bonds into a single credit profile.

    Returns: dict with keys: ticker, leverage, coverage, ratings,
    outstanding_bonds, as_of_date.

    Example: get_credit_summary("AAPL")
    """
    raise NotImplementedError("Will be implemented in Phase 3")
