"""Bond and TRACE transaction tools for WRDS MCP."""

import logging
from typing import Annotated

from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field

from wrds_mcp.db.connection import get_wrds_connection
from wrds_mcp.tools._validation import (
    df_to_records,
    validate_cusip,
    validate_date_range,
    validate_ticker,
)

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

    Returns: list of dicts with cusip, trade_date, trade_time, price, yield_pct,
    volume, buy_sell, bond_symbol.

    Example: get_bond_transactions("AAPL", "2024-01-01", "2024-06-30")
    """
    ticker = validate_ticker(ticker)
    start_date, end_date = validate_date_range(start_date, end_date)

    conn = get_wrds_connection()

    query = """
        SELECT t.cusip_id AS cusip,
               t.trd_exctn_dt AS trade_date,
               t.trd_exctn_tm AS trade_time,
               t.rptd_pr AS price,
               t.yld_pt AS yield_pct,
               t.entrd_vol_qt AS volume,
               t.rpt_side_cd AS buy_sell,
               t.bond_sym_id AS bond_symbol
        FROM trace.trace_enhanced t
        INNER JOIN fisd.fisd_mergedissue fi
            ON t.cusip_id = fi.complete_cusip
        WHERE (UPPER(fi.ticker) = :ticker
               OR fi.issuer_id IN (
                   SELECT DISTINCT fi2.issuer_id
                   FROM fisd.fisd_mergedissue fi2
                   WHERE UPPER(fi2.ticker) = :ticker
               ))
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
            params={"ticker": ticker, "start_date": start_date, "end_date": end_date},
            date_cols=["trade_date"],
        )
    except Exception as e:
        raise ToolError(f"WRDS query failed: {e}")

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

    Queries trace.trace_enhanced for the given CUSIP, aggregating daily
    volume-weighted average yield and price.

    Returns: list of dicts with date, avg_yield, avg_price, total_volume, num_trades.

    Example: get_bond_yield_history("037833AK6", "2024-01-01", "2024-12-31")
    """
    cusip = validate_cusip(cusip)
    start_date, end_date = validate_date_range(start_date, end_date)

    conn = get_wrds_connection()

    query = """
        SELECT trd_exctn_dt AS date,
               SUM(yld_pt * entrd_vol_qt) / NULLIF(SUM(entrd_vol_qt), 0) AS avg_yield,
               SUM(rptd_pr * entrd_vol_qt) / NULLIF(SUM(entrd_vol_qt), 0) AS avg_price,
               SUM(entrd_vol_qt) AS total_volume,
               COUNT(*) AS num_trades
        FROM trace.trace_enhanced
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
        return [{"message": f"No yield data found for CUSIP {cusip} between {start_date} and {end_date}."}]

    return df_to_records(df)


@bonds_mcp.tool
def get_company_bonds(
    ticker: Annotated[str, Field(description="Company ticker symbol")],
    ctx: Context = None,
) -> list[dict]:
    """Get all outstanding bonds for a company.

    Queries fisd.fisd_mergedissue joined with fisd.fisd_mergedissuer
    for corporate bonds matching the ticker. Filters out convertible,
    asset-backed, and exchangeable bonds.

    Returns: list of dicts with cusip, coupon, maturity, offering_amount,
    security_level, bond_type, coupon_type, offering_date.

    Example: get_company_bonds("AAPL")
    """
    ticker = validate_ticker(ticker)

    conn = get_wrds_connection()

    query = """
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
        WHERE (UPPER(fi.ticker) = :ticker
               OR fi.issuer_id IN (
                   SELECT DISTINCT fi2.issuer_id
                   FROM fisd.fisd_mergedissue fi2
                   WHERE UPPER(fi2.ticker) = :ticker
               ))
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
            params={"ticker": ticker},
            date_cols=["maturity", "offering_date"],
        )
    except Exception as e:
        raise ToolError(f"WRDS query failed: {e}")

    if df.empty:
        return [{"message": f"No bonds found for {ticker}."}]

    return df_to_records(df)
