"""Equity/stock market tools for WRDS MCP using CRSP data."""

import logging
from typing import Annotated

import pandas as pd
from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field

from wrds_mcp.db.connection import get_wrds_connection
from wrds_mcp.tools._validation import (
    df_to_records,
    validate_date_range,
    validate_ticker,
)

logger = logging.getLogger(__name__)

equity_mcp = FastMCP("Equity")


def _date_span_days(start_date: str, end_date: str) -> int:
    """Rough day count between two YYYY-MM-DD dates."""
    from datetime import datetime
    s = datetime.strptime(start_date, "%Y-%m-%d")
    e = datetime.strptime(end_date, "%Y-%m-%d")
    return (e - s).days


@equity_mcp.tool
def get_stock_price_history(
    ticker: Annotated[str, Field(description="Stock ticker symbol, e.g. 'F' for Ford")],
    start_date: Annotated[str, Field(description="Start date in YYYY-MM-DD format")],
    end_date: Annotated[str, Field(description="End date in YYYY-MM-DD format")],
    frequency: Annotated[str, Field(description="'daily' or 'monthly'. Auto-selects monthly for ranges > 2 years if not specified.")] = "auto",
    ctx: Context = None,
) -> list[dict]:
    """Get stock price history from CRSP.

    Returns price, return, volume, and market cap data. Automatically
    switches to monthly data for ranges longer than 2 years unless
    daily is explicitly requested.

    Returns: list of dicts with date, close_price, return, volume,
    market_cap, high, low.

    Example: get_stock_price_history("F", "2024-01-01", "2025-12-31")
    """
    ticker = validate_ticker(ticker)
    start_date, end_date = validate_date_range(start_date, end_date)

    conn = get_wrds_connection()
    span = _date_span_days(start_date, end_date)

    use_monthly = frequency == "monthly" or (frequency == "auto" and span > 730)

    if use_monthly:
        query = """
            SELECT ticker, mthcaldt AS date, mthprc AS close_price,
                   mthret AS return, mthvol AS volume, mthcap AS market_cap
            FROM crsp.msf_v2
            WHERE ticker = :ticker
              AND mthcaldt BETWEEN :start_date AND :end_date
            ORDER BY mthcaldt
        """
    else:
        query = """
            SELECT ticker, dlycaldt AS date, dlyclose AS close_price,
                   dlyret AS return, dlyvol AS volume, dlycap AS market_cap,
                   dlyhigh AS high, dlylow AS low, dlyopen AS open
            FROM crsp.dsf_v2
            WHERE ticker = :ticker
              AND dlycaldt BETWEEN :start_date AND :end_date
            ORDER BY dlycaldt
        """

    logger.debug(
        "get_stock_price_history: ticker=%s, start=%s, end=%s, freq=%s",
        ticker, start_date, end_date, "monthly" if use_monthly else "daily",
    )

    try:
        df = conn.raw_sql(query, params={"ticker": ticker, "start_date": start_date, "end_date": end_date}, date_cols=["date"])
    except Exception as e:
        raise ToolError(f"WRDS query failed: {e}")

    if df.empty:
        return [{"message": f"No stock data found for {ticker} between {start_date} and {end_date}."}]

    return df_to_records(df)


@equity_mcp.tool
def get_stock_returns(
    ticker: Annotated[str, Field(description="Stock ticker symbol")],
    start_date: Annotated[str, Field(description="Start date in YYYY-MM-DD format")],
    end_date: Annotated[str, Field(description="End date in YYYY-MM-DD format")],
    ctx: Context = None,
) -> dict:
    """Get cumulative stock return over a period from CRSP.

    Computes total return by compounding daily returns. Also provides
    annualized return, start/end prices, and total volume.

    Returns: dict with ticker, start_date, end_date, cumulative_return,
    annualized_return, start_price, end_price, total_volume, trading_days.

    Example: get_stock_returns("F", "2024-01-01", "2024-12-31")
    """
    ticker = validate_ticker(ticker)
    start_date, end_date = validate_date_range(start_date, end_date)

    conn = get_wrds_connection()

    query = """
        SELECT dlycaldt AS date, dlyclose AS price, dlyret AS return, dlyvol AS volume
        FROM crsp.dsf_v2
        WHERE ticker = :ticker
          AND dlycaldt BETWEEN :start_date AND :end_date
          AND dlyret IS NOT NULL
        ORDER BY dlycaldt
    """

    logger.debug("get_stock_returns: ticker=%s, start=%s, end=%s", ticker, start_date, end_date)

    try:
        df = conn.raw_sql(query, params={"ticker": ticker, "start_date": start_date, "end_date": end_date}, date_cols=["date"])
    except Exception as e:
        raise ToolError(f"WRDS query failed: {e}")

    if df.empty:
        return {"message": f"No stock data found for {ticker} between {start_date} and {end_date}."}

    # Compound daily returns: (1+r1)(1+r2)...(1+rn) - 1
    returns = df["return"].dropna()
    cum_return = float(((1 + returns).prod()) - 1)
    trading_days = len(returns)
    years = trading_days / 252.0

    annualized = float((1 + cum_return) ** (1 / years) - 1) if years > 0 else None

    prices = df["price"].dropna()
    total_vol = df["volume"].dropna().sum()

    return {
        "ticker": ticker,
        "start_date": start_date,
        "end_date": end_date,
        "cumulative_return": round(cum_return, 6),
        "cumulative_return_pct": f"{cum_return * 100:.2f}%",
        "annualized_return": round(annualized, 6) if annualized else None,
        "annualized_return_pct": f"{annualized * 100:.2f}%" if annualized else None,
        "start_price": round(float(prices.iloc[0]), 2) if len(prices) > 0 else None,
        "end_price": round(float(prices.iloc[-1]), 2) if len(prices) > 0 else None,
        "total_volume": int(total_vol),
        "trading_days": trading_days,
    }


@equity_mcp.tool
def get_stock_summary(
    ticker: Annotated[str, Field(description="Stock ticker symbol")],
    ctx: Context = None,
) -> dict:
    """Get a current stock snapshot: latest price, 52-week range, market cap, YTD return.

    Pulls the most recent data available in CRSP and computes key metrics.

    Returns: dict with ticker, latest_date, latest_price, market_cap,
    week_52_high, week_52_low, ytd_return, avg_daily_volume_30d.

    Example: get_stock_summary("F")
    """
    ticker = validate_ticker(ticker)
    conn = get_wrds_connection()

    # Get latest data point and 52-week range
    query = """
        WITH latest AS (
            SELECT MAX(dlycaldt) AS max_date
            FROM crsp.dsf_v2
            WHERE ticker = :ticker
        )
        SELECT d.ticker, d.dlycaldt AS date, d.dlyclose AS price,
               d.dlycap AS market_cap, d.dlyvol AS volume
        FROM crsp.dsf_v2 d, latest l
        WHERE d.ticker = :ticker
          AND d.dlycaldt >= l.max_date - INTERVAL '365 days'
        ORDER BY d.dlycaldt
    """

    logger.debug("get_stock_summary: ticker=%s", ticker)

    try:
        df = conn.raw_sql(query, params={"ticker": ticker}, date_cols=["date"])
    except Exception as e:
        raise ToolError(f"WRDS query failed: {e}")

    if df.empty:
        return {"message": f"No stock data found for {ticker}."}

    latest = df.iloc[-1]
    latest_date = latest["date"]
    prices = df["price"].dropna()

    # YTD return: find first trading day of the latest year
    latest_year = latest_date.year
    ytd_df = df[df["date"].dt.year == latest_year]
    ytd_return = None
    if len(ytd_df) > 1:
        ytd_prices = ytd_df["price"].dropna()
        if len(ytd_prices) > 1:
            ytd_return = round(float((ytd_prices.iloc[-1] / ytd_prices.iloc[0]) - 1), 6)

    # 30-day avg volume
    last_30 = df.tail(30)
    avg_vol = int(last_30["volume"].dropna().mean()) if len(last_30) > 0 else None

    return {
        "ticker": ticker,
        "latest_date": latest_date.isoformat()[:10],
        "latest_price": round(float(latest["price"]), 2) if pd.notna(latest["price"]) else None,
        "market_cap": round(float(latest["market_cap"]), 2) if pd.notna(latest["market_cap"]) else None,
        "market_cap_formatted": _format_large_number(latest["market_cap"]) if pd.notna(latest["market_cap"]) else None,
        "week_52_high": round(float(prices.max()), 2) if len(prices) > 0 else None,
        "week_52_low": round(float(prices.min()), 2) if len(prices) > 0 else None,
        "ytd_return": ytd_return,
        "ytd_return_pct": f"{ytd_return * 100:.2f}%" if ytd_return is not None else None,
        "avg_daily_volume_30d": avg_vol,
    }


def _format_large_number(val) -> str:
    """Format large numbers as human-readable (e.g., $52.1B)."""
    if val is None or pd.isna(val):
        return None
    val = float(val)
    if abs(val) >= 1e12:
        return f"${val/1e12:.1f}T"
    elif abs(val) >= 1e9:
        return f"${val/1e9:.1f}B"
    elif abs(val) >= 1e6:
        return f"${val/1e6:.1f}M"
    return f"${val:,.0f}"
