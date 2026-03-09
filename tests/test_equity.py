"""Tests for equity/stock market tools."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastmcp.exceptions import ToolError

from wrds_mcp.tools.equity import (
    get_stock_price_history,
    get_stock_returns,
    get_stock_summary,
)


class TestGetStockPriceHistory:
    """Tests for get_stock_price_history tool."""

    @patch("wrds_mcp.tools.equity.get_wrds_connection")
    def test_happy_path_daily(self, mock_get_conn):
        df = pd.DataFrame({
            "ticker": ["F"] * 3,
            "date": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
            "close_price": [12.05, 12.10, 11.98],
            "return": [0.005, 0.004, -0.010],
            "volume": [50000000, 45000000, 55000000],
            "market_cap": [48e9, 48.2e9, 47.8e9],
            "high": [12.20, 12.15, 12.10],
            "low": [11.90, 12.00, 11.85],
            "open": [12.00, 12.05, 12.12],
        })
        conn = MagicMock()
        conn.raw_sql.return_value = df
        mock_get_conn.return_value = conn

        result = get_stock_price_history("F", "2024-01-02", "2024-01-04")

        assert isinstance(result, list)
        assert len(result) == 3
        assert result[0]["close_price"] == 12.05
        assert result[0]["ticker"] == "F"
        assert "high" in result[0]
        conn.raw_sql.assert_called_once()

    @patch("wrds_mcp.tools.equity.get_wrds_connection")
    def test_auto_frequency_uses_monthly_for_long_range(self, mock_get_conn):
        df = pd.DataFrame({
            "ticker": ["F"] * 2,
            "date": pd.to_datetime(["2020-01-31", "2020-02-28"]),
            "close_price": [9.50, 9.30],
            "return": [-0.02, -0.021],
            "volume": [1000000000, 950000000],
            "market_cap": [38e9, 37.2e9],
        })
        conn = MagicMock()
        conn.raw_sql.return_value = df
        mock_get_conn.return_value = conn

        # Range > 2 years => auto selects monthly (crsp.msf_v2)
        result = get_stock_price_history("F", "2020-01-01", "2024-12-31")

        assert len(result) == 2
        call_args = conn.raw_sql.call_args
        query = call_args[0][0]
        assert "msf_v2" in query

    @patch("wrds_mcp.tools.equity.get_wrds_connection")
    def test_explicit_daily_frequency_overrides_auto(self, mock_get_conn):
        df = pd.DataFrame({
            "ticker": ["F"],
            "date": pd.to_datetime(["2020-01-02"]),
            "close_price": [9.50],
            "return": [-0.02],
            "volume": [50000000],
            "market_cap": [38e9],
            "high": [9.60],
            "low": [9.40],
            "open": [9.45],
        })
        conn = MagicMock()
        conn.raw_sql.return_value = df
        mock_get_conn.return_value = conn

        # Explicit daily even for long range
        result = get_stock_price_history("F", "2020-01-01", "2024-12-31", frequency="daily")

        call_args = conn.raw_sql.call_args
        query = call_args[0][0]
        assert "dsf_v2" in query

    @patch("wrds_mcp.tools.equity.get_wrds_connection")
    def test_explicit_monthly_frequency(self, mock_get_conn):
        df = pd.DataFrame({
            "ticker": ["F"],
            "date": pd.to_datetime(["2024-01-31"]),
            "close_price": [12.00],
            "return": [0.01],
            "volume": [1000000000],
            "market_cap": [48e9],
        })
        conn = MagicMock()
        conn.raw_sql.return_value = df
        mock_get_conn.return_value = conn

        # Short range but explicit monthly
        result = get_stock_price_history("F", "2024-01-01", "2024-02-28", frequency="monthly")

        call_args = conn.raw_sql.call_args
        query = call_args[0][0]
        assert "msf_v2" in query

    @patch("wrds_mcp.tools.equity.get_wrds_connection")
    def test_empty_results(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame()
        mock_get_conn.return_value = conn

        result = get_stock_price_history("ZZZZ", "2024-01-01", "2024-06-30")

        assert len(result) == 1
        assert "message" in result[0]
        assert "No stock data found" in result[0]["message"]

    def test_empty_ticker(self):
        with pytest.raises(ToolError, match="cannot be empty"):
            get_stock_price_history("", "2024-01-01", "2024-06-30")

    def test_invalid_ticker(self):
        with pytest.raises(ToolError, match="Invalid ticker"):
            get_stock_price_history("F@#$", "2024-01-01", "2024-06-30")

    def test_invalid_date_format(self):
        with pytest.raises(ToolError, match="Expected format"):
            get_stock_price_history("F", "01-01-2024", "2024-06-30")

    def test_date_range_inverted(self):
        with pytest.raises(ToolError, match="must be before"):
            get_stock_price_history("F", "2024-06-30", "2024-01-01")

    def test_invalid_date_value(self):
        with pytest.raises(ToolError, match="not a valid calendar date"):
            get_stock_price_history("F", "2024-02-30", "2024-06-30")

    @patch("wrds_mcp.tools.equity.get_wrds_connection")
    def test_query_failure(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.side_effect = Exception("Query timeout")
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="WRDS query failed"):
            get_stock_price_history("F", "2024-01-01", "2024-06-30")

    @patch("wrds_mcp.tools.equity.get_wrds_connection")
    def test_connection_failure(self, mock_get_conn):
        mock_get_conn.side_effect = ConnectionError("Connection refused")

        with pytest.raises(ConnectionError):
            get_stock_price_history("F", "2024-01-01", "2024-06-30")


class TestGetStockReturns:
    """Tests for get_stock_returns tool."""

    @patch("wrds_mcp.tools.equity.get_wrds_connection")
    def test_happy_path(self, mock_get_conn):
        df = pd.DataFrame({
            "date": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
            "price": [100.0, 102.0, 101.0],
            "return": [0.01, 0.02, -0.0098],
            "volume": [1000000, 1200000, 900000],
        })
        conn = MagicMock()
        conn.raw_sql.return_value = df
        mock_get_conn.return_value = conn

        result = get_stock_returns("F", "2024-01-02", "2024-01-04")

        assert isinstance(result, dict)
        assert result["ticker"] == "F"
        assert result["start_date"] == "2024-01-02"
        assert result["end_date"] == "2024-01-04"
        assert "cumulative_return" in result
        assert "annualized_return" in result
        assert result["start_price"] == 100.0
        assert result["end_price"] == 101.0
        assert result["trading_days"] == 3
        assert result["total_volume"] == 3100000
        assert "cumulative_return_pct" in result

    @patch("wrds_mcp.tools.equity.get_wrds_connection")
    def test_compounding_math(self, mock_get_conn):
        # Two days with 10% return each: (1.1)(1.1) - 1 = 0.21
        df = pd.DataFrame({
            "date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
            "price": [110.0, 121.0],
            "return": [0.10, 0.10],
            "volume": [1000000, 1000000],
        })
        conn = MagicMock()
        conn.raw_sql.return_value = df
        mock_get_conn.return_value = conn

        result = get_stock_returns("F", "2024-01-02", "2024-01-03")

        assert result["cumulative_return"] == 0.21

    @patch("wrds_mcp.tools.equity.get_wrds_connection")
    def test_empty_results(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame()
        mock_get_conn.return_value = conn

        result = get_stock_returns("ZZZZ", "2024-01-01", "2024-06-30")

        assert isinstance(result, dict)
        assert "message" in result
        assert "No stock data found" in result["message"]

    def test_empty_ticker(self):
        with pytest.raises(ToolError, match="cannot be empty"):
            get_stock_returns("", "2024-01-01", "2024-06-30")

    def test_invalid_ticker(self):
        with pytest.raises(ToolError, match="Invalid ticker"):
            get_stock_returns("F@#$", "2024-01-01", "2024-06-30")

    def test_invalid_date_format(self):
        with pytest.raises(ToolError, match="Expected format"):
            get_stock_returns("F", "01-01-2024", "2024-06-30")

    def test_date_range_inverted(self):
        with pytest.raises(ToolError, match="must be before"):
            get_stock_returns("F", "2024-06-30", "2024-01-01")

    @patch("wrds_mcp.tools.equity.get_wrds_connection")
    def test_query_failure(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.side_effect = Exception("Query timeout")
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="WRDS query failed"):
            get_stock_returns("F", "2024-01-01", "2024-06-30")

    @patch("wrds_mcp.tools.equity.get_wrds_connection")
    def test_connection_failure(self, mock_get_conn):
        mock_get_conn.side_effect = ConnectionError("Connection refused")

        with pytest.raises(ConnectionError):
            get_stock_returns("F", "2024-01-01", "2024-06-30")


class TestGetStockSummary:
    """Tests for get_stock_summary tool."""

    @patch("wrds_mcp.tools.equity.get_wrds_connection")
    def test_happy_path(self, mock_get_conn):
        dates = pd.to_datetime([
            "2024-06-01", "2024-06-02", "2024-06-03",
            "2024-12-30", "2024-12-31",
        ])
        df = pd.DataFrame({
            "ticker": ["F"] * 5,
            "date": dates,
            "price": [11.0, 14.0, 12.0, 12.50, 13.00],
            "market_cap": [44e9, 56e9, 48e9, 50e9, 52e9],
            "volume": [50e6, 60e6, 55e6, 45e6, 48e6],
        })
        conn = MagicMock()
        conn.raw_sql.return_value = df
        mock_get_conn.return_value = conn

        result = get_stock_summary("F")

        assert isinstance(result, dict)
        assert result["ticker"] == "F"
        assert result["latest_price"] == 13.00
        assert result["week_52_high"] == 14.0
        assert result["week_52_low"] == 11.0
        assert result["market_cap"] == 52e9
        assert "market_cap_formatted" in result
        assert "avg_daily_volume_30d" in result

    @patch("wrds_mcp.tools.equity.get_wrds_connection")
    def test_ytd_return_calculation(self, mock_get_conn):
        # All dates in same year for YTD calc
        dates = pd.to_datetime([
            "2024-01-02", "2024-01-03", "2024-06-30",
        ])
        df = pd.DataFrame({
            "ticker": ["F"] * 3,
            "date": dates,
            "price": [10.0, 10.5, 12.0],
            "market_cap": [40e9, 42e9, 48e9],
            "volume": [50e6, 55e6, 45e6],
        })
        conn = MagicMock()
        conn.raw_sql.return_value = df
        mock_get_conn.return_value = conn

        result = get_stock_summary("F")

        assert result["ytd_return"] is not None
        # YTD = 12.0 / 10.0 - 1 = 0.2
        assert result["ytd_return"] == 0.2

    @patch("wrds_mcp.tools.equity.get_wrds_connection")
    def test_empty_results(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame()
        mock_get_conn.return_value = conn

        result = get_stock_summary("ZZZZ")

        assert isinstance(result, dict)
        assert "message" in result
        assert "No stock data found" in result["message"]

    def test_empty_ticker(self):
        with pytest.raises(ToolError, match="cannot be empty"):
            get_stock_summary("")

    def test_invalid_ticker(self):
        with pytest.raises(ToolError, match="Invalid ticker"):
            get_stock_summary("F@#$")

    @patch("wrds_mcp.tools.equity.get_wrds_connection")
    def test_query_failure(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.side_effect = Exception("Query timeout")
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="WRDS query failed"):
            get_stock_summary("F")

    @patch("wrds_mcp.tools.equity.get_wrds_connection")
    def test_connection_failure(self, mock_get_conn):
        mock_get_conn.side_effect = ConnectionError("Connection refused")

        with pytest.raises(ConnectionError):
            get_stock_summary("F")

    @patch("wrds_mcp.tools.equity.get_wrds_connection")
    def test_market_cap_formatted_billions(self, mock_get_conn):
        dates = pd.to_datetime(["2024-12-31"])
        df = pd.DataFrame({
            "ticker": ["F"],
            "date": dates,
            "price": [13.00],
            "market_cap": [52.1e9],
            "volume": [48e6],
        })
        conn = MagicMock()
        conn.raw_sql.return_value = df
        mock_get_conn.return_value = conn

        result = get_stock_summary("F")

        assert result["market_cap_formatted"] == "$52.1B"

    @patch("wrds_mcp.tools.equity.get_wrds_connection")
    def test_nan_price_handled(self, mock_get_conn):
        dates = pd.to_datetime(["2024-12-31"])
        df = pd.DataFrame({
            "ticker": ["F"],
            "date": dates,
            "price": [float("nan")],
            "market_cap": [52e9],
            "volume": [48e6],
        })
        conn = MagicMock()
        conn.raw_sql.return_value = df
        mock_get_conn.return_value = conn

        result = get_stock_summary("F")

        assert result["latest_price"] is None
