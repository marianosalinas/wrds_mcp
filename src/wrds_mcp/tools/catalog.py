"""Data catalog discovery tool for WRDS MCP.

Provides Claude with live metadata about available datasets, date ranges,
and tool routing guidance. This is the first tool Claude should call when
uncertain about what data exists or which tool to use.
"""

import logging
from typing import Annotated

from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field

from wrds_mcp.db.connection import get_wrds_connection

logger = logging.getLogger(__name__)

catalog_mcp = FastMCP("Catalog")

# Cache for the session to avoid repeated metadata queries
_catalog_cache: dict | None = None


def _query_date_range(conn, schema: str, table: str, date_col: str) -> dict:
    """Query the min/max date and row count for a table."""
    try:
        df = conn.raw_sql(
            f"SELECT MIN({date_col}) AS earliest, MAX({date_col}) AS latest, "
            f"COUNT(*) AS row_count FROM {schema}.{table}"
        )
        if df.empty:
            return {"earliest": None, "latest": None, "row_count": 0}
        row = df.iloc[0]
        return {
            "earliest": str(row["earliest"])[:10] if row["earliest"] else None,
            "latest": str(row["latest"])[:10] if row["latest"] else None,
            "row_count": int(row["row_count"]),
        }
    except Exception as e:
        logger.debug("Could not query %s.%s: %s", schema, table, e)
        return {"earliest": None, "latest": None, "row_count": 0, "error": str(e)}


def _check_schema_exists(conn, schema: str) -> bool:
    """Check if a schema exists and is accessible."""
    try:
        df = conn.raw_sql(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = :schema LIMIT 1",
            params={"schema": schema},
        )
        return not df.empty
    except Exception:
        return False


@catalog_mcp.tool
def get_data_catalog(
    refresh: Annotated[bool, Field(description="Force refresh cached catalog")] = False,
    ctx: Context = None,
) -> dict:
    """Get a live catalog of all available WRDS datasets with date ranges.

    Call this FIRST when you need to understand what data is available,
    what date ranges are covered, and which tool to use for a query.

    The catalog is cached for the session. Pass refresh=True to re-query.

    Returns a dict organized by category (equity, bonds, ratings, financials,
    loans) with dataset names, date ranges, row counts, and recommended tools.

    Example: get_data_catalog()
    """
    global _catalog_cache
    if _catalog_cache is not None and not refresh:
        return _catalog_cache

    conn = get_wrds_connection()
    catalog = {"_note": "Live WRDS data catalog. Date ranges reflect actual data availability."}

    # --- Equity (CRSP) ---
    equity = {}
    if _check_schema_exists(conn, "crsp"):
        equity["daily_stock_data"] = {
            "source": "crsp.dsf_v2",
            "description": "Daily stock prices, returns, volume, market cap for all US equities",
            "coverage": _query_date_range(conn, "crsp", "dsf_v2", "dlycaldt"),
            "key_columns": ["permno", "ticker", "dlycaldt", "dlyprc", "dlyret", "dlyvol", "dlycap", "dlyclose", "dlyhigh", "dlylow", "dlyopen"],
            "use_tool": "get_stock_price_history or get_stock_returns",
        }
        equity["monthly_stock_data"] = {
            "source": "crsp.msf_v2",
            "description": "Monthly stock prices, returns, market cap",
            "coverage": _query_date_range(conn, "crsp", "msf_v2", "mthcaldt"),
            "key_columns": ["permno", "ticker", "mthcaldt", "mthprc", "mthret", "mthcap", "mthvol"],
            "use_tool": "get_stock_price_history (auto-selects monthly for long ranges)",
        }
        equity["sp500_membership"] = {
            "source": "crsp.dsp500list_v2",
            "description": "S&P 500 index membership history",
        }
    catalog["equity"] = equity if equity else {"available": False}

    # --- Bonds (TRACE + FISD) ---
    bonds = {}
    if _check_schema_exists(conn, "trace"):
        bonds["trace_realtime"] = {
            "source": "trace.trace",
            "description": "Raw FINRA TRACE bond transactions — most current data, needs filtering",
            "coverage": _query_date_range(conn, "trace", "trace", "trd_exctn_dt"),
            "note": "Filter trc_st != 'C' and != 'W' to exclude cancellations. Volume is text (ascii_rptd_vol_tx).",
            "use_tool": "get_bond_price_history (auto-routes here for recent dates)",
        }
        bonds["trace_enhanced_clean"] = {
            "source": "wrdsapps_bondret.trace_enhanced_clean",
            "description": "Cleaned TRACE — reversals removed, research quality",
            "coverage": _query_date_range(conn, "wrdsapps_bondret", "trace_enhanced_clean", "trd_exctn_dt"),
            "use_tool": "get_bond_price_history (auto-routes here for historical dates)",
        }
    if _check_schema_exists(conn, "fisd"):
        bonds["bond_characteristics"] = {
            "source": "fisd.fisd_mergedissue + fisd_mergedissuer",
            "description": "Bond issue details: CUSIP, coupon, maturity, seniority, bond type",
            "use_tool": "get_company_bonds",
        }
        bonds["bond_covenants"] = {
            "source": "fisd.fisd_bondholder_protective + fisd_call_schedule + fisd_put_schedule + fisd_sinking_fund",
            "description": "Covenant details: change of control, cross-default, negative pledge, call/put schedules",
            "use_tool": "get_bond_covenants",
        }
        bonds["bond_ratings_fisd"] = {
            "source": "fisd.fisd_ratings",
            "description": "Bond-level ratings from multiple agencies",
        }
    if _check_schema_exists(conn, "wrdsapps_bondret"):
        bonds["bond_returns"] = {
            "source": "wrdsapps_bondret.bondret",
            "description": "Monthly bond returns, yield, spread, duration, price, and S&P/Moody's/Fitch ratings",
            "coverage": _query_date_range(conn, "wrdsapps_bondret", "bondret", "date"),
            "note": "Best source for current credit ratings (multi-agency) and bond performance",
            "use_tool": "get_bond_returns or get_credit_ratings",
        }
    catalog["bonds"] = bonds if bonds else {"available": False}

    # --- Ratings ---
    ratings = {}
    ratings["current_ratings"] = {
        "source": "wrdsapps_bondret.bondret (latest month)",
        "description": "S&P, Moody's, and Fitch ratings — most current available",
        "note": "Pulled from bond returns table. Much more current than Compustat adsprate (which ended Feb 2017).",
        "use_tool": "get_credit_ratings",
    }
    ratings["historical_ratings_compustat"] = {
        "source": "comp.adsprate",
        "description": "S&P issuer ratings — historical only",
        "coverage": _query_date_range(conn, "comp", "adsprate", "datadate"),
        "note": "S&P discontinued licensing to Compustat after Feb 2017. Use bondret for anything recent.",
    }
    catalog["ratings"] = ratings

    # --- Financials (Compustat) ---
    financials = {}
    if _check_schema_exists(conn, "comp"):
        financials["annual_fundamentals"] = {
            "source": "comp.funda",
            "description": "Annual financial statements: assets, debt, EBITDA, interest, cash flow",
            "coverage": _query_date_range(conn, "comp", "funda", "datadate"),
            "use_tool": "get_leverage_metrics, get_coverage_ratios, get_liquidity_metrics",
        }
        financials["quarterly_fundamentals"] = {
            "source": "comp.fundq",
            "description": "Quarterly financial statements",
            "coverage": _query_date_range(conn, "comp", "fundq", "datadate"),
            "note": "Not yet implemented in tools — available for future TTM calculations.",
        }
    catalog["financials"] = financials if financials else {"available": False}

    # --- Loans (DealScan) ---
    loans = {}
    if _check_schema_exists(conn, "dealscan"):
        loans["syndicated_loans"] = {
            "source": "dealscan.facility + package + currfacpricing",
            "description": "Syndicated loan terms: spreads, maturity, facility type, amount",
            "use_tool": "get_loan_terms",
        }
        loans["loan_covenants"] = {
            "source": "dealscan.financialcovenant + networthcovenant",
            "description": "Financial and net worth covenants on syndicated loans",
            "use_tool": "get_loan_covenants",
        }
    catalog["loans"] = loans if loans else {"available": False}

    # --- Composite tools ---
    catalog["composite_tools"] = {
        "get_credit_summary": "Full credit snapshot: leverage, coverage, current ratings, bonds, covenants",
        "get_company_overview": "Everything: stock performance + full credit profile",
    }

    _catalog_cache = catalog
    return catalog
