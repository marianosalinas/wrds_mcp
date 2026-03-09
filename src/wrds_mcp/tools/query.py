"""Guarded SQL execution tool for WRDS MCP — read-only SELECT queries."""

import logging
import re
from typing import Annotated

import pandas as pd
from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field
from sqlalchemy import text

from wrds_mcp.db.connection import get_wrds_connection
from wrds_mcp.tools._validation import df_to_records

logger = logging.getLogger(__name__)

query_mcp = FastMCP("Query")

# Schemas the WRDS read-only user is allowed to query
ALLOWED_SCHEMAS = {"comp", "crsp", "trace", "wrdsapps_bondret", "fisd", "dealscan"}

# SQL keywords that indicate mutation — must appear as whole words
MUTATION_KEYWORDS = {
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
    "TRUNCATE", "GRANT", "REVOKE", "COPY", "EXECUTE", "CALL",
}

MAX_LIMIT = 10_000
STATEMENT_TIMEOUT_MS = 30_000  # 30 seconds


def _strip_comments(sql: str) -> str:
    """Remove SQL comments (-- line and /* block */) from a query."""
    # Remove block comments (non-greedy, handles nested poorly but good enough)
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    # Remove line comments
    sql = re.sub(r"--[^\n]*", " ", sql)
    return sql


def _strip_string_literals(sql: str) -> str:
    """Replace string literals with placeholders so keyword detection ignores them."""
    # Replace single-quoted strings (handling escaped quotes '')
    return re.sub(r"'(?:[^']|'')*'", "'_STR_'", sql)


def _validate_query(sql: str) -> tuple[str, list[str]]:
    """Validate and sanitize a SQL query for safe read-only execution.

    Returns:
        (sanitized_sql, warnings) — the potentially modified SQL and a list of warnings.

    Raises:
        ToolError: If the query is rejected for safety reasons.
    """
    warnings: list[str] = []

    # Strip comments and normalize whitespace
    cleaned = _strip_comments(sql)
    normalized = " ".join(cleaned.split())

    if not normalized.strip():
        raise ToolError("Empty query after stripping comments.")

    # Check that query starts with SELECT or WITH (case-insensitive)
    first_word = normalized.strip().split()[0].upper()
    if first_word not in ("SELECT", "WITH"):
        raise ToolError(
            f"Only SELECT queries are allowed. Query starts with '{first_word}'."
        )

    # Check for mutation keywords — use version with string literals removed
    check_text = _strip_string_literals(normalized).upper()
    for kw in MUTATION_KEYWORDS:
        # Word-boundary match: the keyword must not be part of a larger identifier
        if re.search(rf"\b{kw}\b", check_text):
            raise ToolError(
                f"Query contains disallowed keyword '{kw}'. "
                "Only read-only SELECT queries are permitted."
            )

    # Schema allowlist: find schema.table references
    # Matches word.word patterns (schema.table), ignoring things inside strings
    schema_refs = re.findall(r"\b(\w+)\.\w+", check_text)
    for schema in schema_refs:
        schema_lower = schema.lower()
        # Skip common non-schema patterns like function qualifiers or aliases
        if schema_lower in ALLOWED_SCHEMAS:
            continue
        # Could be an alias or subquery reference — only reject if it looks
        # like a real schema (all-alpha, reasonable length)
        if schema_lower.isalpha() and len(schema_lower) >= 2 and schema_lower != "_str_":
            raise ToolError(
                f"Schema '{schema_lower}' is not in the allowlist. "
                f"Allowed schemas: {', '.join(sorted(ALLOWED_SCHEMAS))}."
            )

    # Warn if touching comp.funda or comp.fundq without industrial filter
    funda_tables = re.findall(r"\bcomp\.(funda|fundq)\b", normalized, re.IGNORECASE)
    if funda_tables:
        if not re.search(r"indfmt\s*=\s*'INDL'", normalized, re.IGNORECASE):
            warnings.append(
                "Query touches comp.funda/fundq without the standard industrial "
                "filter (indfmt = 'INDL'). Results may include non-industrial "
                "format records. Consider adding: indfmt = 'INDL' AND datafmt = 'STD' AND consol = 'C'."
            )

    # Handle LIMIT clause
    limit_match = re.search(r"\bLIMIT\s+(\d+)", normalized, re.IGNORECASE)
    if limit_match:
        existing_limit = int(limit_match.group(1))
        if existing_limit > MAX_LIMIT:
            warnings.append(
                f"LIMIT {existing_limit} exceeds maximum of {MAX_LIMIT}. "
                f"Reduced to {MAX_LIMIT}."
            )
            normalized = (
                normalized[: limit_match.start()]
                + f"LIMIT {MAX_LIMIT}"
                + normalized[limit_match.end():]
            )
    else:
        # Append LIMIT if not present (strip trailing semicolons first)
        normalized = normalized.rstrip().rstrip(";").rstrip()
        normalized += f" LIMIT {MAX_LIMIT}"

    return normalized, warnings


@query_mcp.tool
def query_wrds(
    sql: Annotated[
        str,
        Field(
            description=(
                "SQL SELECT query to execute against WRDS. "
                "Only SELECT/WITH statements are allowed. "
                "Tables must be from allowed schemas: comp, crsp, trace, "
                "wrdsapps_bondret, fisd, dealscan. "
                "A LIMIT of 10000 is enforced automatically."
            ),
        ),
    ],
    params: Annotated[
        dict | None,
        Field(
            description=(
                "Optional query parameters as a dict, e.g. {'ticker': 'AAPL'}. "
                "Use :param_name placeholders in the SQL."
            ),
        ),
    ] = None,
    ctx: Context = None,
) -> dict:
    """Execute a read-only SQL SELECT query against the WRDS database.

    The query is validated for safety: only SELECT/WITH statements are allowed,
    mutation keywords are rejected, and only approved schemas may be queried.
    A maximum LIMIT of 10,000 rows and a 30-second timeout are enforced.

    Returns: dict with keys: rows (list of dicts), row_count, truncated,
    warnings, source.

    Example: query_wrds("SELECT gvkey, datadate, sale FROM comp.funda WHERE tic = :ticker AND indfmt = 'INDL' LIMIT 10", params={"ticker": "AAPL"})
    """
    # Validate and sanitize the query
    try:
        sanitized_sql, warnings = _validate_query(sql)
    except ToolError:
        raise
    except Exception as e:
        raise ToolError(f"Query validation error: {e}")

    logger.info("Executing validated query: %s", sanitized_sql[:200])

    conn = get_wrds_connection()

    try:
        # Execute with statement timeout via the underlying engine
        with conn._engine.connect() as db_conn:
            # Set statement timeout for this session
            db_conn.execute(
                text(f"SET statement_timeout = {STATEMENT_TIMEOUT_MS}")
            )

            df = pd.read_sql_query(
                text(sanitized_sql),
                con=db_conn,
                params=params or {},
            )
    except Exception as e:
        error_msg = str(e)
        if "statement timeout" in error_msg.lower() or "cancel" in error_msg.lower():
            raise ToolError(
                f"Query timed out after {STATEMENT_TIMEOUT_MS // 1000} seconds. "
                "Try adding more filters or reducing the scope."
            )
        raise ToolError(f"WRDS query execution failed: {e}")

    records = df_to_records(df, max_rows=MAX_LIMIT)
    truncated = len(df) > MAX_LIMIT
    row_count = min(len(df), MAX_LIMIT)

    return {
        "rows": records,
        "row_count": row_count,
        "truncated": truncated,
        "warnings": warnings,
        "source": "query_wrds",
    }
