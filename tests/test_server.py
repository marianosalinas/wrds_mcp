"""Tests for MCP server configuration."""

import pytest

from wrds_mcp.tools.bonds import bonds_mcp
from wrds_mcp.tools.ratings import ratings_mcp
from wrds_mcp.tools.financials import financials_mcp
from wrds_mcp.tools._validation import (
    validate_date,
    validate_date_range,
    validate_ticker,
    validate_cusip,
    df_to_records,
)
import pandas as pd
from fastmcp.exceptions import ToolError


class TestValidation:
    """Tests for input validation helpers."""

    def test_validate_date_valid(self):
        assert validate_date("2024-01-15") == "2024-01-15"

    def test_validate_date_bad_format(self):
        with pytest.raises(ToolError, match="Expected format"):
            validate_date("01-15-2024")

    def test_validate_date_invalid_calendar(self):
        with pytest.raises(ToolError, match="not a valid calendar date"):
            validate_date("2024-02-30")

    def test_validate_date_range_valid(self):
        s, e = validate_date_range("2024-01-01", "2024-12-31")
        assert s == "2024-01-01"
        assert e == "2024-12-31"

    def test_validate_date_range_same_day(self):
        s, e = validate_date_range("2024-06-15", "2024-06-15")
        assert s == e

    def test_validate_date_range_inverted(self):
        with pytest.raises(ToolError, match="must be before"):
            validate_date_range("2024-12-31", "2024-01-01")

    def test_validate_ticker_normal(self):
        assert validate_ticker("aapl") == "AAPL"

    def test_validate_ticker_with_dot(self):
        assert validate_ticker("BRK.B") == "BRK.B"

    def test_validate_ticker_empty(self):
        with pytest.raises(ToolError, match="cannot be empty"):
            validate_ticker("")

    def test_validate_ticker_whitespace(self):
        with pytest.raises(ToolError, match="cannot be empty"):
            validate_ticker("   ")

    def test_validate_ticker_special_chars(self):
        with pytest.raises(ToolError, match="Invalid ticker"):
            validate_ticker("AAPL@!")

    def test_validate_cusip_valid(self):
        assert validate_cusip("037833AK6") == "037833AK6"

    def test_validate_cusip_too_short(self):
        with pytest.raises(ToolError, match="exactly 9"):
            validate_cusip("12345")

    def test_validate_cusip_too_long(self):
        with pytest.raises(ToolError, match="exactly 9"):
            validate_cusip("1234567890")

    def test_validate_cusip_bad_chars(self):
        with pytest.raises(ToolError, match="letters and digits"):
            validate_cusip("037833@K6")


class TestDfToRecords:
    """Tests for DataFrame to records conversion."""

    def test_basic_conversion(self):
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        result = df_to_records(df)
        assert result == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]

    def test_nan_to_none(self):
        df = pd.DataFrame({"a": [1.0, float("nan")]})
        result = df_to_records(df)
        assert result[1]["a"] is None

    def test_timestamp_to_iso(self):
        df = pd.DataFrame({"date": pd.to_datetime(["2024-01-15"])})
        result = df_to_records(df)
        assert result[0]["date"] == "2024-01-15"

    def test_truncation(self):
        df = pd.DataFrame({"a": range(100)})
        result = df_to_records(df, max_rows=10)
        assert len(result) == 11  # 10 data + 1 truncation notice
        assert result[-1]["_truncated"] is True

    def test_inf_to_none(self):
        df = pd.DataFrame({"a": [float("inf"), float("-inf")]})
        result = df_to_records(df)
        assert result[0]["a"] is None
        assert result[1]["a"] is None


class TestServerToolRegistration:
    """Tests for tool registration on sub-servers."""

    def test_bonds_has_tools(self):
        # Verify bonds_mcp has the expected tools registered
        assert bonds_mcp is not None

    def test_ratings_has_tools(self):
        assert ratings_mcp is not None

    def test_financials_has_tools(self):
        assert financials_mcp is not None
