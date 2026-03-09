"""Tests for financial metrics tools."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastmcp.exceptions import ToolError

from wrds_mcp.tools.financials import (
    get_leverage_metrics,
    get_coverage_ratios,
    get_liquidity_metrics,
    get_credit_summary,
    _safe_divide,
    _safe_float,
)


class TestHelpers:
    """Tests for helper functions."""

    def test_safe_divide_normal(self):
        assert _safe_divide(10, 2) == 5.0

    def test_safe_divide_zero(self):
        assert _safe_divide(10, 0) is None

    def test_safe_divide_none(self):
        assert _safe_divide(None, 5) is None
        assert _safe_divide(5, None) is None

    def test_safe_divide_nan(self):
        assert _safe_divide(float("nan"), 5) is None

    def test_safe_float_normal(self):
        assert _safe_float(3.14159) == 3.14

    def test_safe_float_none(self):
        assert _safe_float(None) is None

    def test_safe_float_nan(self):
        assert _safe_float(float("nan")) is None


def _make_funda_df(**overrides):
    """Create a minimal Compustat funda DataFrame for testing."""
    data = {
        "gvkey": ["001690"],
        "datadate": pd.to_datetime(["2024-09-30"]),
        "fyear": [2024],
        "dltt": [96813.0],
        "dlc": [17382.0],
        "oibdp": [132916.0],
        "che": [29943.0],
        "at": [364980.0],
        "xint": [3755.0],
        "xrent": [None],
        "act": [152987.0],
        "lct": [176392.0],
        "ivst": [35228.0],
    }
    data.update(overrides)
    return pd.DataFrame(data)


class TestGetLeverageMetrics:
    """Tests for get_leverage_metrics tool."""

    @patch("wrds_mcp.tools.financials.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.financials.get_wrds_connection")
    def test_happy_path(self, mock_get_conn, mock_resolve):
        mock_resolve.return_value = "001690"
        conn = MagicMock()
        conn.raw_sql.return_value = _make_funda_df()
        mock_get_conn.return_value = conn

        result = get_leverage_metrics("AAPL", periods=1)

        assert len(result) == 1
        r = result[0]
        assert r["fiscal_year"] == 2024
        assert r["total_debt"] == 114195.0  # 96813 + 17382
        assert r["ebitda"] == 132916.0
        assert r["debt_to_ebitda"] == round(114195.0 / 132916.0, 4)
        assert r["net_debt"] == round(114195.0 - 29943.0, 2)
        assert r["total_assets"] == 364980.0

    @patch("wrds_mcp.tools.financials.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.financials.get_wrds_connection")
    def test_ticker_not_found(self, mock_get_conn, mock_resolve):
        mock_resolve.return_value = None
        mock_get_conn.return_value = MagicMock()

        with pytest.raises(ToolError, match="not found in Compustat"):
            get_leverage_metrics("ZZZZ")

    @patch("wrds_mcp.tools.financials.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.financials.get_wrds_connection")
    def test_empty_results(self, mock_get_conn, mock_resolve):
        mock_resolve.return_value = "001690"
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame()
        mock_get_conn.return_value = conn

        result = get_leverage_metrics("AAPL")

        assert "message" in result[0]

    @patch("wrds_mcp.tools.financials.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.financials.get_wrds_connection")
    def test_nan_values(self, mock_get_conn, mock_resolve):
        mock_resolve.return_value = "001690"
        conn = MagicMock()
        conn.raw_sql.return_value = _make_funda_df(oibdp=[float("nan")])
        mock_get_conn.return_value = conn

        result = get_leverage_metrics("AAPL", periods=1)

        assert result[0]["ebitda"] is None
        assert result[0]["debt_to_ebitda"] is None

    def test_invalid_ticker(self):
        with pytest.raises(ToolError):
            get_leverage_metrics("AAPL@!")

    @patch("wrds_mcp.tools.financials.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.financials.get_wrds_connection")
    def test_query_failure(self, mock_get_conn, mock_resolve):
        mock_resolve.return_value = "001690"
        conn = MagicMock()
        conn.raw_sql.side_effect = Exception("Timeout")
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="WRDS query failed"):
            get_leverage_metrics("AAPL")


class TestGetCoverageRatios:
    """Tests for get_coverage_ratios tool."""

    @patch("wrds_mcp.tools.financials.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.financials.get_wrds_connection")
    def test_happy_path(self, mock_get_conn, mock_resolve):
        mock_resolve.return_value = "001690"
        conn = MagicMock()
        conn.raw_sql.return_value = _make_funda_df()
        mock_get_conn.return_value = conn

        result = get_coverage_ratios("AAPL", periods=1)

        assert len(result) == 1
        r = result[0]
        assert r["ebitda"] == 132916.0
        assert r["interest_expense"] == 3755.0
        assert r["interest_coverage"] == round(132916.0 / 3755.0, 4)

    @patch("wrds_mcp.tools.financials.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.financials.get_wrds_connection")
    def test_with_rent(self, mock_get_conn, mock_resolve):
        mock_resolve.return_value = "001690"
        conn = MagicMock()
        conn.raw_sql.return_value = _make_funda_df(xrent=[1000.0])
        mock_get_conn.return_value = conn

        result = get_coverage_ratios("AAPL", periods=1)

        r = result[0]
        assert r["rental_expense"] == 1000.0
        expected_fcc = round((132916.0 + 1000.0) / (3755.0 + 1000.0), 4)
        assert r["fixed_charge_coverage"] == expected_fcc

    @patch("wrds_mcp.tools.financials.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.financials.get_wrds_connection")
    def test_zero_interest(self, mock_get_conn, mock_resolve):
        mock_resolve.return_value = "001690"
        conn = MagicMock()
        conn.raw_sql.return_value = _make_funda_df(xint=[0.0])
        mock_get_conn.return_value = conn

        result = get_coverage_ratios("AAPL", periods=1)

        assert result[0]["interest_coverage"] is None

    @patch("wrds_mcp.tools.financials.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.financials.get_wrds_connection")
    def test_empty_results(self, mock_get_conn, mock_resolve):
        mock_resolve.return_value = "001690"
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame()
        mock_get_conn.return_value = conn

        result = get_coverage_ratios("AAPL")

        assert "message" in result[0]

    @patch("wrds_mcp.tools.financials.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.financials.get_wrds_connection")
    def test_query_failure(self, mock_get_conn, mock_resolve):
        mock_resolve.return_value = "001690"
        conn = MagicMock()
        conn.raw_sql.side_effect = Exception("DB error")
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="WRDS query failed"):
            get_coverage_ratios("AAPL")


class TestGetLiquidityMetrics:
    """Tests for get_liquidity_metrics tool."""

    @patch("wrds_mcp.tools.financials.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.financials.get_wrds_connection")
    def test_happy_path(self, mock_get_conn, mock_resolve):
        mock_resolve.return_value = "001690"
        conn = MagicMock()
        conn.raw_sql.return_value = _make_funda_df()
        mock_get_conn.return_value = conn

        result = get_liquidity_metrics("AAPL", periods=1)

        assert len(result) == 1
        r = result[0]
        assert r["current_assets"] == 152987.0
        assert r["current_liabilities"] == 176392.0
        assert r["current_ratio"] == round(152987.0 / 176392.0, 4)
        assert r["cash_and_equivalents"] == 29943.0
        assert r["short_term_investments"] == 35228.0

    @patch("wrds_mcp.tools.financials.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.financials.get_wrds_connection")
    def test_zero_current_liabilities(self, mock_get_conn, mock_resolve):
        mock_resolve.return_value = "001690"
        conn = MagicMock()
        conn.raw_sql.return_value = _make_funda_df(lct=[0.0])
        mock_get_conn.return_value = conn

        result = get_liquidity_metrics("AAPL", periods=1)

        assert result[0]["current_ratio"] is None

    @patch("wrds_mcp.tools.financials.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.financials.get_wrds_connection")
    def test_empty_results(self, mock_get_conn, mock_resolve):
        mock_resolve.return_value = "001690"
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame()
        mock_get_conn.return_value = conn

        result = get_liquidity_metrics("AAPL")

        assert "message" in result[0]

    @patch("wrds_mcp.tools.financials.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.financials.get_wrds_connection")
    def test_query_failure(self, mock_get_conn, mock_resolve):
        mock_resolve.return_value = "001690"
        conn = MagicMock()
        conn.raw_sql.side_effect = Exception("Timeout")
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="WRDS query failed"):
            get_liquidity_metrics("AAPL")


class TestGetCreditSummary:
    """Tests for get_credit_summary tool."""

    @patch("wrds_mcp.tools.bonds.get_company_bonds")
    @patch("wrds_mcp.tools.ratings.get_credit_ratings")
    @patch("wrds_mcp.tools.financials.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.financials.get_wrds_connection")
    def test_happy_path(self, mock_get_conn, mock_resolve, mock_ratings, mock_bonds):
        mock_resolve.return_value = "001690"
        conn = MagicMock()
        conn.raw_sql.return_value = _make_funda_df()
        mock_get_conn.return_value = conn

        mock_ratings.return_value = {
            "ticker": "AAPL",
            "gvkey": "001690",
            "rating": "AA+",
            "rating_date": "2017-01-05",
        }
        mock_bonds.return_value = [
            {"cusip": "037833AK6", "coupon": 2.25, "maturity": "2026-05-01"},
        ]

        result = get_credit_summary("AAPL")

        assert result["ticker"] == "AAPL"
        assert result["leverage"] is not None
        assert result["coverage"] is not None
        assert result["ratings"]["rating"] == "AA+"
        assert result["outstanding_bonds_count"] == 1
        assert result["as_of_date"] is not None

    @patch("wrds_mcp.tools.bonds.get_company_bonds")
    @patch("wrds_mcp.tools.ratings.get_credit_ratings")
    @patch("wrds_mcp.tools.financials.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.financials.get_wrds_connection")
    def test_no_data(self, mock_get_conn, mock_resolve, mock_ratings, mock_bonds):
        mock_resolve.return_value = "999999"
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame()
        mock_get_conn.return_value = conn

        mock_ratings.return_value = {"ticker": "NEWCO", "rating": None, "message": "No ratings"}
        mock_bonds.return_value = [{"message": "No bonds found"}]

        result = get_credit_summary("NEWCO")

        assert result["leverage"] is None
        assert result["outstanding_bonds_count"] == 0
        assert result["outstanding_bonds"] == []

    def test_invalid_ticker(self):
        with pytest.raises(ToolError):
            get_credit_summary("")
