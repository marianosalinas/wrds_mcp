"""Financial metrics tools for WRDS MCP (leverage, coverage, liquidity)."""

import logging
from typing import Annotated

import pandas as pd
from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field

from wrds_mcp.db.connection import get_wrds_connection, resolve_ticker_to_gvkey
from wrds_mcp.tools._validation import validate_ticker

logger = logging.getLogger(__name__)

financials_mcp = FastMCP("Financials")

# Standard Compustat filter for annual industrial data
FUNDA_FILTER = """
    indfmt = 'INDL'
    AND datafmt = 'STD'
    AND consol = 'C'
    AND curcd = 'USD'
"""


def _safe_divide(numerator, denominator):
    """Safe division that returns None for NaN/zero denominators."""
    if denominator is None or pd.isna(denominator) or denominator == 0:
        return None
    if numerator is None or pd.isna(numerator):
        return None
    return round(float(numerator / denominator), 4)


def _safe_float(val):
    """Convert to float, returning None for NaN/None."""
    if val is None or pd.isna(val):
        return None
    return round(float(val), 2)


def _query_funda(conn, gvkey: str, periods: int, columns: str) -> pd.DataFrame:
    """Query comp.funda with standard filters."""
    query = f"""
        SELECT gvkey, datadate, fyear, {columns}
        FROM comp.funda
        WHERE gvkey = :gvkey
          AND {FUNDA_FILTER}
        ORDER BY datadate DESC
        LIMIT :periods
    """
    logger.debug("Querying comp.funda: gvkey=%s, periods=%d", gvkey, periods)
    return conn.raw_sql(
        query,
        params={"gvkey": gvkey, "periods": periods},
        date_cols=["datadate"],
    )


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
    ticker = validate_ticker(ticker)
    conn = get_wrds_connection()

    gvkey = resolve_ticker_to_gvkey(conn, ticker)
    if gvkey is None:
        raise ToolError(f"Ticker '{ticker}' not found in Compustat.")

    try:
        df = _query_funda(conn, gvkey, periods, "dltt, dlc, oibdp, che, at")
    except Exception as e:
        raise ToolError(f"WRDS query failed: {e}")

    if df.empty:
        return [{"message": f"No financial data found for {ticker}."}]

    results = []
    for _, row in df.iterrows():
        total_debt = _safe_float(
            (row.get("dltt") or 0) + (row.get("dlc") or 0)
        ) if not (pd.isna(row.get("dltt")) and pd.isna(row.get("dlc"))) else None
        ebitda = _safe_float(row.get("oibdp"))
        che = _safe_float(row.get("che"))
        net_debt = round(total_debt - che, 2) if total_debt is not None and che is not None else None

        results.append({
            "fiscal_year": int(row["fyear"]) if not pd.isna(row.get("fyear")) else None,
            "datadate": row["datadate"].isoformat()[:10] if hasattr(row["datadate"], "isoformat") else str(row["datadate"]),
            "total_debt": total_debt,
            "ebitda": ebitda,
            "debt_to_ebitda": _safe_divide(total_debt, ebitda),
            "net_debt": net_debt,
            "net_debt_to_ebitda": _safe_divide(net_debt, ebitda),
            "total_assets": _safe_float(row.get("at")),
        })

    return sorted(results, key=lambda x: x.get("datadate") or "")


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
    ticker = validate_ticker(ticker)
    conn = get_wrds_connection()

    gvkey = resolve_ticker_to_gvkey(conn, ticker)
    if gvkey is None:
        raise ToolError(f"Ticker '{ticker}' not found in Compustat.")

    try:
        df = _query_funda(conn, gvkey, periods, "oibdp, xint, xrent")
    except Exception as e:
        raise ToolError(f"WRDS query failed: {e}")

    if df.empty:
        return [{"message": f"No financial data found for {ticker}."}]

    results = []
    for _, row in df.iterrows():
        ebitda = _safe_float(row.get("oibdp"))
        xint = _safe_float(row.get("xint"))
        xrent = _safe_float(row.get("xrent"))

        interest_coverage = _safe_divide(ebitda, xint)

        # Fixed charge coverage: (EBITDA + rent) / (interest + rent)
        fcc = None
        if ebitda is not None and xint is not None:
            rent = xrent if xrent is not None else 0
            numerator = ebitda + rent
            denominator = xint + rent
            fcc = _safe_divide(numerator, denominator)

        results.append({
            "fiscal_year": int(row["fyear"]) if not pd.isna(row.get("fyear")) else None,
            "datadate": row["datadate"].isoformat()[:10] if hasattr(row["datadate"], "isoformat") else str(row["datadate"]),
            "ebitda": ebitda,
            "interest_expense": xint,
            "interest_coverage": interest_coverage,
            "rental_expense": xrent,
            "fixed_charge_coverage": fcc,
        })

    return sorted(results, key=lambda x: x.get("datadate") or "")


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
    ticker = validate_ticker(ticker)
    conn = get_wrds_connection()

    gvkey = resolve_ticker_to_gvkey(conn, ticker)
    if gvkey is None:
        raise ToolError(f"Ticker '{ticker}' not found in Compustat.")

    try:
        df = _query_funda(conn, gvkey, periods, "act, lct, che, ivst")
    except Exception as e:
        raise ToolError(f"WRDS query failed: {e}")

    if df.empty:
        return [{"message": f"No financial data found for {ticker}."}]

    results = []
    for _, row in df.iterrows():
        act = _safe_float(row.get("act"))
        lct = _safe_float(row.get("lct"))

        results.append({
            "fiscal_year": int(row["fyear"]) if not pd.isna(row.get("fyear")) else None,
            "datadate": row["datadate"].isoformat()[:10] if hasattr(row["datadate"], "isoformat") else str(row["datadate"]),
            "current_ratio": _safe_divide(act, lct),
            "cash_and_equivalents": _safe_float(row.get("che")),
            "short_term_investments": _safe_float(row.get("ivst")),
            "current_assets": act,
            "current_liabilities": lct,
        })

    return sorted(results, key=lambda x: x.get("datadate") or "")


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
    ticker = validate_ticker(ticker)

    from wrds_mcp.tools.bonds import get_company_bonds
    from wrds_mcp.tools.ratings import get_credit_ratings

    leverage = get_leverage_metrics(ticker, periods=1)
    coverage = get_coverage_ratios(ticker, periods=1)
    ratings = get_credit_ratings(ticker)
    bonds = get_company_bonds(ticker)

    # Extract the most recent data point (skip "message" entries)
    leverage_data = leverage[0] if leverage and "message" not in leverage[0] else None
    coverage_data = coverage[0] if coverage and "message" not in coverage[0] else None

    as_of = None
    if leverage_data:
        as_of = leverage_data.get("datadate")
    elif coverage_data:
        as_of = coverage_data.get("datadate")

    return {
        "ticker": ticker,
        "as_of_date": as_of,
        "leverage": leverage_data,
        "coverage": coverage_data,
        "ratings": ratings,
        "outstanding_bonds_count": len(bonds) if bonds and "message" not in bonds[0] else 0,
        "outstanding_bonds": bonds if bonds and "message" not in bonds[0] else [],
    }
