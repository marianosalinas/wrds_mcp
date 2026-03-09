"""Tests for bond tools."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastmcp.exceptions import ToolError

from wrds_mcp.tools.bonds import (
    get_bond_transactions,
    get_bond_yield_history,
    get_company_bonds,
)


@pytest.fixture
def mock_conn(sample_trace_df, sample_fisd_df):
    """Mock WRDS connection for bond queries."""
    conn = MagicMock()
    conn.raw_sql = MagicMock(return_value=sample_trace_df)
    return conn


class TestGetBondTransactions:
    """Tests for get_bond_transactions tool."""

    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_happy_path(self, mock_get_conn):
        # The SQL query aliases columns, so mock must use aliased names
        df = pd.DataFrame({
            "cusip": ["037833AK6"] * 3,
            "trade_date": pd.to_datetime(["2024-01-15", "2024-01-16", "2024-01-17"]),
            "trade_time": ["10:30:00", "14:15:00", "09:45:00"],
            "price": [99.5, 99.6, 99.4],
            "yield_pct": [4.52, 4.51, 4.53],
            "volume": [1000000, 500000, 2000000],
            "buy_sell": ["B", "S", "B"],
            "bond_symbol": ["AAPL.GX"] * 3,
        })
        conn = MagicMock()
        conn.raw_sql.return_value = df
        mock_get_conn.return_value = conn

        result = get_bond_transactions("AAPL", "2024-01-15", "2024-01-17")

        assert isinstance(result, list)
        assert len(result) == 3
        assert result[0]["cusip"] == "037833AK6"
        assert result[0]["price"] == 99.5
        assert "trade_date" in result[0]
        conn.raw_sql.assert_called_once()

    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_empty_results(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame()
        mock_get_conn.return_value = conn

        result = get_bond_transactions("ZZZZ", "2024-01-01", "2024-06-30")

        assert len(result) == 1
        assert "message" in result[0]
        assert "No TRACE transactions" in result[0]["message"]

    def test_invalid_date_format(self):
        with pytest.raises(ToolError, match="Expected format"):
            get_bond_transactions("AAPL", "01-15-2024", "2024-06-30")

    def test_date_range_inverted(self):
        with pytest.raises(ToolError, match="must be before"):
            get_bond_transactions("AAPL", "2024-06-30", "2024-01-01")

    def test_empty_ticker(self):
        with pytest.raises(ToolError, match="cannot be empty"):
            get_bond_transactions("", "2024-01-01", "2024-06-30")

    def test_invalid_ticker(self):
        with pytest.raises(ToolError, match="Invalid ticker"):
            get_bond_transactions("AAPL@#$", "2024-01-01", "2024-06-30")

    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_connection_failure(self, mock_get_conn):
        mock_get_conn.side_effect = ConnectionError("Connection refused")

        with pytest.raises(ConnectionError):
            get_bond_transactions("AAPL", "2024-01-01", "2024-06-30")

    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_query_failure(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.side_effect = Exception("Query timeout")
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="WRDS query failed"):
            get_bond_transactions("AAPL", "2024-01-01", "2024-06-30")

    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_nan_handling(self, mock_get_conn):
        df = pd.DataFrame({
            "cusip": ["037833AK6"],
            "trade_date": pd.to_datetime(["2024-01-15"]),
            "trade_time": ["10:30:00"],
            "price": [99.5],
            "yield_pct": [float("nan")],
            "volume": [1000000],
            "buy_sell": ["B"],
            "bond_symbol": ["AAPL.GX"],
        })
        conn = MagicMock()
        conn.raw_sql.return_value = df
        mock_get_conn.return_value = conn

        result = get_bond_transactions("AAPL", "2024-01-15", "2024-01-15")

        assert result[0]["yield_pct"] is None

    def test_invalid_date_value(self):
        with pytest.raises(ToolError, match="not a valid calendar date"):
            get_bond_transactions("AAPL", "2024-02-30", "2024-06-30")


class TestGetBondYieldHistory:
    """Tests for get_bond_yield_history tool."""

    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_happy_path(self, mock_get_conn):
        df = pd.DataFrame({
            "date": pd.to_datetime(["2024-01-15", "2024-01-16", "2024-01-17"]),
            "avg_yield": [4.52, 4.51, 4.49],
            "avg_price": [99.5, 99.55, 99.8],
            "total_volume": [1500000, 2750000, 1500000],
            "num_trades": [2, 2, 1],
        })
        conn = MagicMock()
        conn.raw_sql.return_value = df
        mock_get_conn.return_value = conn

        result = get_bond_yield_history("037833AK6", "2024-01-15", "2024-01-17")

        assert len(result) == 3
        assert result[0]["avg_yield"] == 4.52
        assert result[2]["num_trades"] == 1

    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_empty_results(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame()
        mock_get_conn.return_value = conn

        result = get_bond_yield_history("000000000", "2024-01-01", "2024-12-31")

        assert "message" in result[0]

    def test_invalid_cusip_length(self):
        with pytest.raises(ToolError, match="exactly 9 characters"):
            get_bond_yield_history("12345", "2024-01-01", "2024-12-31")

    def test_invalid_cusip_chars(self):
        with pytest.raises(ToolError, match="letters and digits"):
            get_bond_yield_history("037833@K6", "2024-01-01", "2024-12-31")

    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_query_failure(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.side_effect = Exception("Timeout")
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="WRDS query failed"):
            get_bond_yield_history("037833AK6", "2024-01-01", "2024-12-31")


class TestGetCompanyBonds:
    """Tests for get_company_bonds tool."""

    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_happy_path(self, mock_get_conn, sample_fisd_df):
        conn = MagicMock()
        conn.raw_sql.return_value = sample_fisd_df.rename(columns={
            "complete_cusip": "cusip",
            "offering_amt": "offering_amount",
        })
        mock_get_conn.return_value = conn

        result = get_company_bonds("AAPL")

        assert len(result) == 3
        assert result[0]["coupon"] == 2.25
        assert result[0]["security_level"] == "SEN"

    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_empty_results(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame()
        mock_get_conn.return_value = conn

        result = get_company_bonds("ZZZZ")

        assert "message" in result[0]
        assert "No bonds found" in result[0]["message"]

    def test_empty_ticker(self):
        with pytest.raises(ToolError, match="cannot be empty"):
            get_company_bonds("  ")

    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_query_failure(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.side_effect = Exception("Connection lost")
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="WRDS query failed"):
            get_company_bonds("AAPL")
