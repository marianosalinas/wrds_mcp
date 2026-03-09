"""Tests for comps table tool."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastmcp.exceptions import ToolError

import wrds_mcp.tools.screening as screening_mod
from wrds_mcp.tools.comps import get_comps_table


@pytest.fixture(autouse=True)
def reset_latest_month_cache():
    screening_mod._latest_full_month = None
    yield
    screening_mod._latest_full_month = None


def _make_conn_with_responses(*dfs):
    """Create a mock connection returning sequential DataFrames."""
    conn = MagicMock()
    # First call: latest month detection, then the data queries
    latest_month_df = pd.DataFrame({"date": ["2025-03-31"], "n": [800]})
    conn.raw_sql.side_effect = [latest_month_df, *dfs]
    return conn


class TestGetCompsTable:
    @patch("wrds_mcp.tools.comps.get_wrds_connection")
    def test_happy_path_single_ticker(self, mock_get_conn):
        ratings_df = pd.DataFrame({
            "ticker": ["F"],
            "sp_rating": ["BB+"],
            "moody_rating": ["Ba1"],
            "fitch_rating": [None],
            "rating_cat": ["BB"],
            "rating_class": ["1.HY"],
        })
        fin_df = pd.DataFrame({
            "ticker": ["F"],
            "company_name": ["Ford Motor Company"],
            "sic_code": [3711],
            "revenue": [170000.0],
            "ebitda": [12000.0],
            "total_debt": [100000.0],
            "net_debt": [80000.0],
            "cash": [20000.0],
            "leverage": [8.33],
            "interest_coverage": [3.5],
            "market_cap": [40000.0],
            "financials_date": ["2024-12-31"],
        })
        bonds_df = pd.DataFrame({
            "ticker": ["F"],
            "bond_count": [25],
            "total_amount_outstanding": [30000.0],
            "avg_spread": [200.5],
            "avg_yield": [5.8],
            "avg_duration": [4.2],
        })
        eq_df = pd.DataFrame({
            "ticker": ["F"],
            "ret_1mo": [0.02],
            "ret_3mo": [0.05],
            "ret_6mo": [0.08],
            "ret_12mo": [0.15],
        })
        conn = _make_conn_with_responses(ratings_df, fin_df, bonds_df, eq_df)
        mock_get_conn.return_value = conn

        result = get_comps_table(["F"])

        assert result["as_of_date"] == "2025-03-31"
        assert result["count"] == 1
        assert result["tickers"] == ["F"]
        comp = result["comps"][0]
        assert comp["ticker"] == "F"
        assert comp["sp_rating"] == "BB+"
        assert comp["leverage"] == 8.33
        assert comp["bond_count"] == 25
        assert comp["equity_return_1mo"] == 0.02

    @patch("wrds_mcp.tools.comps.get_wrds_connection")
    def test_multiple_tickers(self, mock_get_conn):
        ratings_df = pd.DataFrame({
            "ticker": ["F", "GM"],
            "sp_rating": ["BB+", "BBB-"],
            "moody_rating": ["Ba1", "Baa3"],
            "fitch_rating": [None, None],
            "rating_cat": ["BB", "BBB"],
            "rating_class": ["1.HY", "0.IG"],
        })
        fin_df = pd.DataFrame({
            "ticker": ["F", "GM"],
            "company_name": ["Ford", "General Motors"],
            "sic_code": [3711, 3711],
            "revenue": [170000.0, 180000.0],
            "ebitda": [12000.0, 14000.0],
            "total_debt": [100000.0, 90000.0],
            "net_debt": [80000.0, 70000.0],
            "cash": [20000.0, 20000.0],
            "leverage": [8.33, 6.43],
            "interest_coverage": [3.5, 4.0],
            "market_cap": [40000.0, 50000.0],
            "financials_date": ["2024-12-31", "2024-12-31"],
        })
        bonds_df = pd.DataFrame({
            "ticker": ["F", "GM"],
            "bond_count": [25, 20],
            "total_amount_outstanding": [30000.0, 25000.0],
            "avg_spread": [200.5, 150.3],
            "avg_yield": [5.8, 5.2],
            "avg_duration": [4.2, 3.8],
        })
        eq_df = pd.DataFrame({
            "ticker": ["F", "GM"],
            "ret_1mo": [0.02, 0.01],
            "ret_3mo": [0.05, 0.03],
            "ret_6mo": [0.08, 0.06],
            "ret_12mo": [0.15, 0.12],
        })
        conn = _make_conn_with_responses(ratings_df, fin_df, bonds_df, eq_df)
        mock_get_conn.return_value = conn

        result = get_comps_table(["F", "GM"])

        assert result["count"] == 2
        assert result["comps"][0]["ticker"] == "F"
        assert result["comps"][1]["ticker"] == "GM"
        assert result["comps"][1]["market_cap"] == 50000.0

    def test_empty_tickers_raises(self):
        with pytest.raises(ToolError, match="at least one ticker"):
            get_comps_table([])

    def test_too_many_tickers_raises(self):
        with pytest.raises(ToolError, match="Maximum 20"):
            get_comps_table([f"T{i}" for i in range(21)])

    def test_invalid_ticker_raises(self):
        with pytest.raises(ToolError, match="Invalid ticker"):
            get_comps_table(["F@#$"])

    @patch("wrds_mcp.tools.comps.get_wrds_connection")
    def test_missing_data_for_ticker(self, mock_get_conn):
        """Ticker not in any result set — should still appear with minimal data."""
        conn = _make_conn_with_responses(
            pd.DataFrame(),  # no ratings
            pd.DataFrame(),  # no financials
            pd.DataFrame(),  # no bonds
            pd.DataFrame(),  # no equity
        )
        mock_get_conn.return_value = conn

        result = get_comps_table(["ZZZZ"])

        assert result["count"] == 1
        assert result["comps"][0]["ticker"] == "ZZZZ"
        assert "sp_rating" not in result["comps"][0]

    @patch("wrds_mcp.tools.comps.get_wrds_connection")
    def test_partial_data(self, mock_get_conn):
        """Some queries fail but others succeed."""
        ratings_df = pd.DataFrame({
            "ticker": ["F"],
            "sp_rating": ["BB+"],
            "moody_rating": ["Ba1"],
            "fitch_rating": [None],
            "rating_cat": ["BB"],
            "rating_class": ["1.HY"],
        })
        latest_month_df = pd.DataFrame({"date": ["2025-03-31"], "n": [800]})
        conn = MagicMock()
        # latest month OK, ratings OK, financials fails, bonds fails, equity fails
        conn.raw_sql.side_effect = [
            latest_month_df,
            ratings_df,
            Exception("timeout"),
            Exception("timeout"),
            Exception("timeout"),
        ]
        mock_get_conn.return_value = conn

        result = get_comps_table(["F"])

        assert result["comps"][0]["sp_rating"] == "BB+"
        assert "leverage" not in result["comps"][0]

    @patch("wrds_mcp.tools.comps.get_wrds_connection")
    def test_ticker_case_insensitive(self, mock_get_conn):
        conn = _make_conn_with_responses(
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        )
        mock_get_conn.return_value = conn

        result = get_comps_table(["f"])

        assert result["tickers"] == ["F"]
