"""Syndicated loan tools for WRDS MCP using DealScan data."""

import logging
from typing import Annotated

import pandas as pd
from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field

from wrds_mcp.db.connection import get_wrds_connection
from wrds_mcp.tools._validation import df_to_records, validate_ticker

logger = logging.getLogger(__name__)

loans_mcp = FastMCP("Loans")


@loans_mcp.tool
def get_loan_terms(
    ticker: Annotated[str, Field(description="Company ticker symbol, e.g. 'F' for Ford")],
    ctx: Context = None,
) -> list[dict]:
    """Get syndicated loan facility terms from DealScan.

    Queries DealScan for loan facilities associated with the company,
    including spreads, maturity, facility type, and amount.

    Returns: list of dicts with facility_id, facility_type, facility_amt,
    facility_start_date, facility_end_date, spread, base_rate, currency,
    borrower_name, deal_active_date.

    Example: get_loan_terms("F")
    """
    ticker = validate_ticker(ticker)
    conn = get_wrds_connection()

    # DealScan links borrowers to facilities via package
    # Search by ticker in the company table
    query = """
        SELECT f.facilityid AS facility_id,
               f.facilitytypedesc AS facility_type,
               f.facilityamt AS facility_amt,
               f.facilitystartdate AS facility_start_date,
               f.facilityenddate AS facility_end_date,
               f.currency,
               p.dealactivedate AS deal_active_date,
               b.borrowercompanyid,
               b.borrowername AS borrower_name,
               cfp.allindrawnspread AS spread,
               cfp.baserate AS base_rate
        FROM dealscan.facility f
        INNER JOIN dealscan.package p
            ON f.packageid = p.packageid
        INNER JOIN dealscan.borrower b
            ON p.packageid = b.packageid
        LEFT JOIN dealscan.currfacpricing cfp
            ON f.facilityid = cfp.facilityid
        INNER JOIN dealscan.company c
            ON b.borrowercompanyid = c.companyid
        WHERE UPPER(c.ticker) = :ticker
        ORDER BY p.dealactivedate DESC, f.facilityid
    """

    logger.debug("get_loan_terms: ticker=%s", ticker)

    try:
        df = conn.raw_sql(
            query,
            params={"ticker": ticker},
            date_cols=["facility_start_date", "facility_end_date", "deal_active_date"],
        )
    except Exception as e:
        raise ToolError(f"WRDS DealScan query failed: {e}")

    if df.empty:
        return [{"message": f"No syndicated loan data found for {ticker} in DealScan."}]

    # Deduplicate (multiple pricing rows per facility)
    df = df.drop_duplicates(subset=["facility_id", "spread", "base_rate"])

    return df_to_records(df)


@loans_mcp.tool
def get_loan_covenants(
    ticker: Annotated[str, Field(description="Company ticker symbol")],
    ctx: Context = None,
) -> list[dict]:
    """Get financial covenants on syndicated loans from DealScan.

    Queries DealScan for financial covenants (debt/EBITDA, interest coverage,
    etc.) and net worth covenants attached to the company's loan facilities.

    Returns: list of dicts with facility_id, facility_type, covenant_type,
    initial_ratio, deal_active_date.

    Example: get_loan_covenants("F")
    """
    ticker = validate_ticker(ticker)
    conn = get_wrds_connection()

    # Financial covenants
    fin_query = """
        SELECT f.facilityid AS facility_id,
               f.facilitytypedesc AS facility_type,
               fc.covenanttype AS covenant_type,
               fc.initialratio AS initial_ratio,
               p.dealactivedate AS deal_active_date
        FROM dealscan.financialcovenant fc
        INNER JOIN dealscan.facility f
            ON fc.facilityid = f.facilityid
        INNER JOIN dealscan.package p
            ON f.packageid = p.packageid
        INNER JOIN dealscan.borrower b
            ON p.packageid = b.packageid
        INNER JOIN dealscan.company c
            ON b.borrowercompanyid = c.companyid
        WHERE UPPER(c.ticker) = :ticker
        ORDER BY p.dealactivedate DESC, f.facilityid
    """

    # Net worth covenants
    nw_query = """
        SELECT f.facilityid AS facility_id,
               f.facilitytypedesc AS facility_type,
               nwc.nwtype AS covenant_type,
               nwc.nwinitialamt AS initial_amount,
               p.dealactivedate AS deal_active_date
        FROM dealscan.networthcovenant nwc
        INNER JOIN dealscan.facility f
            ON nwc.facilityid = f.facilityid
        INNER JOIN dealscan.package p
            ON f.packageid = p.packageid
        INNER JOIN dealscan.borrower b
            ON p.packageid = b.packageid
        INNER JOIN dealscan.company c
            ON b.borrowercompanyid = c.companyid
        WHERE UPPER(c.ticker) = :ticker
        ORDER BY p.dealactivedate DESC, f.facilityid
    """

    logger.debug("get_loan_covenants: ticker=%s", ticker)

    try:
        fin_df = conn.raw_sql(
            fin_query,
            params={"ticker": ticker},
            date_cols=["deal_active_date"],
        )
        nw_df = conn.raw_sql(
            nw_query,
            params={"ticker": ticker},
            date_cols=["deal_active_date"],
        )
    except Exception as e:
        raise ToolError(f"WRDS DealScan query failed: {e}")

    results = []

    if not fin_df.empty:
        for _, row in fin_df.iterrows():
            results.append({
                "facility_id": int(row["facility_id"]) if pd.notna(row.get("facility_id")) else None,
                "facility_type": row.get("facility_type"),
                "covenant_category": "financial",
                "covenant_type": row.get("covenant_type"),
                "initial_ratio": float(row["initial_ratio"]) if pd.notna(row.get("initial_ratio")) else None,
                "deal_active_date": row["deal_active_date"].isoformat()[:10] if hasattr(row["deal_active_date"], "isoformat") else str(row["deal_active_date"]),
            })

    if not nw_df.empty:
        for _, row in nw_df.iterrows():
            results.append({
                "facility_id": int(row["facility_id"]) if pd.notna(row.get("facility_id")) else None,
                "facility_type": row.get("facility_type"),
                "covenant_category": "net_worth",
                "covenant_type": row.get("covenant_type"),
                "initial_amount": float(row["initial_amount"]) if pd.notna(row.get("initial_amount")) else None,
                "deal_active_date": row["deal_active_date"].isoformat()[:10] if hasattr(row["deal_active_date"], "isoformat") else str(row["deal_active_date"]),
            })

    if not results:
        return [{"message": f"No loan covenants found for {ticker} in DealScan."}]

    return results
