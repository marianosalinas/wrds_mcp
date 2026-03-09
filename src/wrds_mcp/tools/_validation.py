"""Input validation and data conversion helpers shared across tool modules."""

import math
import re
from datetime import date, datetime

import pandas as pd
from fastmcp.exceptions import ToolError

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

MAX_ROWS = 10000


def validate_date(value: str, name: str = "date") -> str:
    """Validate a date string is in YYYY-MM-DD format and is a real date.

    Returns the validated date string.
    Raises ToolError if invalid.
    """
    if not DATE_RE.match(value):
        raise ToolError(
            f"Invalid {name}: '{value}'. Expected format: YYYY-MM-DD (e.g. 2024-01-15)."
        )
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        raise ToolError(f"Invalid {name}: '{value}' is not a valid calendar date.")
    return value


def validate_date_range(start_date: str, end_date: str) -> tuple[str, str]:
    """Validate and return a (start_date, end_date) pair.

    Raises ToolError if start_date > end_date.
    """
    start = validate_date(start_date, "start_date")
    end = validate_date(end_date, "end_date")
    if start > end:
        raise ToolError(
            f"start_date ({start}) must be before or equal to end_date ({end})."
        )
    return start, end


def validate_ticker(ticker: str) -> str:
    """Validate and normalize a ticker symbol.

    Returns the uppercased, stripped ticker.
    Raises ToolError if empty or contains invalid characters.
    """
    cleaned = ticker.strip().upper()
    if not cleaned:
        raise ToolError("Ticker symbol cannot be empty.")
    if not re.match(r"^[A-Z0-9.\-]+$", cleaned):
        raise ToolError(
            f"Invalid ticker '{ticker}'. Ticker must contain only letters, digits, dots, or hyphens."
        )
    return cleaned


def validate_cusip(cusip: str) -> str:
    """Validate a CUSIP identifier.

    Returns the stripped CUSIP.
    Raises ToolError if not 9 characters or contains invalid characters.
    """
    cleaned = cusip.strip().upper()
    if len(cleaned) != 9:
        raise ToolError(
            f"Invalid CUSIP '{cusip}'. CUSIP must be exactly 9 characters (got {len(cleaned)})."
        )
    if not re.match(r"^[A-Z0-9]+$", cleaned):
        raise ToolError(
            f"Invalid CUSIP '{cusip}'. CUSIP must contain only letters and digits."
        )
    return cleaned


def df_to_records(df: pd.DataFrame, max_rows: int = MAX_ROWS) -> list[dict]:
    """Convert a DataFrame to a list of JSON-serializable dicts.

    Handles NaN -> None, NaT -> None, Timestamp -> ISO string,
    and truncates to max_rows with a warning.
    """
    truncated = False
    if len(df) > max_rows:
        df = df.head(max_rows)
        truncated = True

    records = []
    for _, row in df.iterrows():
        record = {}
        for col, val in row.items():
            if pd.isna(val):
                record[col] = None
            elif isinstance(val, (pd.Timestamp, date, datetime)):
                record[col] = val.isoformat()[:10]
            elif isinstance(val, float) and (math.isinf(val) or math.isnan(val)):
                record[col] = None
            else:
                record[col] = val
        records.append(record)

    if truncated:
        records.append({"_truncated": True, "_total_rows": len(df), "_max_rows": max_rows})

    return records
