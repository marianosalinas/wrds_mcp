"""Tests for ratings tools (v2 — bondret primary, Compustat fallback)."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastmcp.exceptions import ToolError

from wrds_mcp.tools.ratings import (
    get_credit_ratings,
    get_ratings_history,
    _rating_direction,
)


class TestRatingDirection:
    """Tests for the rating direction helper."""

    def test_upgrade(self):
        assert _rating_direction("A", "A+") == "upgrade"

    def test_downgrade(self):
        assert _rating_direction("AA", "A+") == "downgrade"

    def test_affirmed(self):
        assert _rating_direction("BBB+", "BBB+") == "affirmed"

    def test_initial(self):
        assert _rating_direction(None, "A") == "initial"

    def test_withdrawn(self):
        assert _rating_direction("A", None) == "withdrawn"

    def test_unknown_rating(self):
        assert _rating_direction("A", "XYZ") == "unknown"


def _make_bondret_df():
    """Create a sample bondret DataFrame for current ratings."""
    return pd.DataFrame({
        "company_symbol": ["AAPL"],
        "date": pd.to_datetime(["2025-12-31"]),
        "r_sp": ["AA+"],
        "r_mr": ["Aa1"],
        "r_fr": ["AA+"],
        "n_sp": [2],
        "n_mr": [2],
        "n_fr": [2],
        "rating_num": [2.0],
        "rating_cat": ["IG"],
        "rating_class": ["AA"],
    })


class TestGetCreditRatings:
    """Tests for get_credit_ratings tool (bondret primary source)."""

    @patch("wrds_mcp.tools.ratings.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.ratings.get_wrds_connection")
    def test_happy_path_bondret(self, mock_get_conn, mock_resolve):
        """Primary path: bondret has data."""
        conn = MagicMock()
        conn.raw_sql.return_value = _make_bondret_df()
        mock_get_conn.return_value = conn

        result = get_credit_ratings("AAPL")

        assert result["ticker"] == "AAPL"
        assert result["sp_rating"] == "AA+"
        assert result["moody_rating"] == "Aa1"
        assert result["fitch_rating"] == "AA+"
        assert result["as_of_date"] == "2025-12-31"
        assert result["source"] == "wrdsapps_bondret.bondret"
        assert result["rating_category"] == "IG"

    @patch("wrds_mcp.tools.ratings.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.ratings.get_wrds_connection")
    def test_fallback_compustat(self, mock_get_conn, mock_resolve):
        """Fallback: bondret empty, Compustat has S&P rating."""
        mock_resolve.return_value = "001690"
        conn = MagicMock()
        # First call (bondret) returns empty, second call (Compustat) returns data
        conn.raw_sql.side_effect = [
            pd.DataFrame(),  # bondret empty
            pd.DataFrame({
                "gvkey": ["001690"],
                "datadate": pd.to_datetime(["2017-01-05"]),
                "splticrm": ["AA+"],
                "spsdrm": [None],
                "spsticrm": ["A-1+"],
            }),
        ]
        mock_get_conn.return_value = conn

        result = get_credit_ratings("AAPL")

        assert result["ticker"] == "AAPL"
        assert result["sp_rating"] == "AA+"
        assert result["moody_rating"] is None
        assert result["source"] == "comp.adsprate"

    @patch("wrds_mcp.tools.ratings.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.ratings.get_wrds_connection")
    def test_ticker_not_found(self, mock_get_conn, mock_resolve):
        """Neither bondret nor Compustat has the ticker."""
        mock_resolve.return_value = None
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame()  # bondret empty
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="not found"):
            get_credit_ratings("ZZZZ")

    @patch("wrds_mcp.tools.ratings.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.ratings.get_wrds_connection")
    def test_no_ratings_data(self, mock_get_conn, mock_resolve):
        """Compustat gvkey found but no ratings rows."""
        mock_resolve.return_value = "999999"
        conn = MagicMock()
        # bondret empty, Compustat empty
        conn.raw_sql.side_effect = [pd.DataFrame(), pd.DataFrame()]
        mock_get_conn.return_value = conn

        result = get_credit_ratings("NEWCO")

        assert result["sp_rating"] is None
        assert "message" in result

    def test_empty_ticker(self):
        with pytest.raises(ToolError, match="cannot be empty"):
            get_credit_ratings("")

    @patch("wrds_mcp.tools.ratings.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.ratings.get_wrds_connection")
    def test_bondret_query_fails_falls_back(self, mock_get_conn, mock_resolve):
        """If bondret query throws, fallback to Compustat gracefully."""
        mock_resolve.return_value = "001690"
        conn = MagicMock()
        # First call raises exception (bondret fails), second returns Compustat data
        conn.raw_sql.side_effect = [
            Exception("bondret timeout"),
            pd.DataFrame({
                "gvkey": ["001690"],
                "datadate": pd.to_datetime(["2017-01-05"]),
                "splticrm": ["AA+"],
                "spsdrm": [None],
                "spsticrm": ["A-1+"],
            }),
        ]
        mock_get_conn.return_value = conn

        result = get_credit_ratings("AAPL")
        assert result["sp_rating"] == "AA+"
        assert result["source"] == "comp.adsprate"

    @patch("wrds_mcp.tools.ratings.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.ratings.get_wrds_connection")
    def test_compustat_query_failure(self, mock_get_conn, mock_resolve):
        """If Compustat query also fails, raise ToolError."""
        mock_resolve.return_value = "001690"
        conn = MagicMock()
        conn.raw_sql.side_effect = [
            pd.DataFrame(),  # bondret empty
            Exception("Timeout"),  # Compustat fails
        ]
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="WRDS query failed"):
            get_credit_ratings("AAPL")


def _make_bondret_history_df():
    """Create a sample bondret DataFrame for ratings history."""
    return pd.DataFrame({
        "date": pd.to_datetime(["2024-01-31", "2024-02-29", "2024-06-30", "2024-12-31"]),
        "r_sp": ["A", "A", "A+", "A+"],
        "r_mr": ["A2", "A2", "A1", "A1"],
        "r_fr": ["A", "A", "A", "A+"],
        "rating_cat": ["IG", "IG", "IG", "IG"],
        "rating_class": ["A", "A", "A", "A"],
    })


class TestGetRatingsHistory:
    """Tests for get_ratings_history tool (bondret primary source)."""

    @patch("wrds_mcp.tools.ratings.get_wrds_connection")
    def test_happy_path(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.return_value = _make_bondret_history_df()
        mock_get_conn.return_value = conn

        result = get_ratings_history("AAPL", "2024-01-01", "2024-12-31")

        # Should only include rows where ratings changed (not Feb which matches Jan)
        assert len(result) >= 2
        # First entry should be initial
        assert result[0]["sp_rating"] == "A"
        # Should have a change entry for the A -> A+ upgrade
        upgrade_entries = [r for r in result if "upgrade" in (r.get("changes") or "")]
        assert len(upgrade_entries) >= 1

    @patch("wrds_mcp.tools.ratings.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.ratings.get_wrds_connection")
    def test_falls_back_to_compustat(self, mock_get_conn, mock_resolve):
        """When bondret is empty, falls back to Compustat."""
        mock_resolve.return_value = "001690"
        conn = MagicMock()
        # First call (bondret) empty, second call (Compustat) has data
        conn.raw_sql.side_effect = [
            pd.DataFrame(),  # bondret empty
            pd.DataFrame({
                "gvkey": ["001690"] * 2,
                "datadate": pd.to_datetime(["2012-01-01", "2014-06-15"]),
                "splticrm": ["AA", "AA+"],
            }),
        ]
        mock_get_conn.return_value = conn

        result = get_ratings_history("AAPL", "2010-01-01", "2017-01-01")

        assert len(result) == 2
        assert result[0]["sp_rating"] == "AA"
        assert result[1]["sp_rating"] == "AA+"

    @patch("wrds_mcp.tools.ratings.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.ratings.get_wrds_connection")
    def test_empty_results(self, mock_get_conn, mock_resolve):
        mock_resolve.return_value = "001690"
        conn = MagicMock()
        conn.raw_sql.side_effect = [pd.DataFrame(), pd.DataFrame()]
        mock_get_conn.return_value = conn

        result = get_ratings_history("AAPL", "2018-01-01", "2020-01-01")

        assert "message" in result[0]

    @patch("wrds_mcp.tools.ratings.get_wrds_connection")
    def test_query_failure(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.side_effect = Exception("Network error")
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="WRDS query failed"):
            get_ratings_history("AAPL", "2010-01-01", "2017-01-01")

    def test_invalid_dates(self):
        with pytest.raises(ToolError, match="Expected format"):
            get_ratings_history("AAPL", "bad-date", "2017-01-01")

    def test_date_range_inverted(self):
        with pytest.raises(ToolError, match="must be before"):
            get_ratings_history("AAPL", "2017-01-01", "2010-01-01")
