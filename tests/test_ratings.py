"""Tests for ratings tools."""

from unittest.mock import MagicMock, patch, call

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


class TestGetCreditRatings:
    """Tests for get_credit_ratings tool."""

    @patch("wrds_mcp.tools.ratings.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.ratings.get_wrds_connection")
    def test_happy_path(self, mock_get_conn, mock_resolve):
        mock_resolve.return_value = "001690"
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame({
            "gvkey": ["001690"],
            "datadate": pd.to_datetime(["2017-01-05"]),
            "splticrm": ["AA+"],
            "spsdrm": [None],
            "spsticrm": ["A-1+"],
        })
        mock_get_conn.return_value = conn

        result = get_credit_ratings("AAPL")

        assert result["ticker"] == "AAPL"
        assert result["rating"] == "AA+"
        assert result["rating_date"] == "2017-01-05"
        assert result["short_term_rating"] == "A-1+"
        assert result["subordinated_rating"] is None

    @patch("wrds_mcp.tools.ratings.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.ratings.get_wrds_connection")
    def test_ticker_not_found(self, mock_get_conn, mock_resolve):
        mock_resolve.return_value = None
        mock_get_conn.return_value = MagicMock()

        with pytest.raises(ToolError, match="not found in Compustat"):
            get_credit_ratings("ZZZZ")

    @patch("wrds_mcp.tools.ratings.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.ratings.get_wrds_connection")
    def test_no_ratings_data(self, mock_get_conn, mock_resolve):
        mock_resolve.return_value = "999999"
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame()
        mock_get_conn.return_value = conn

        result = get_credit_ratings("NEWCO")

        assert result["rating"] is None
        assert "message" in result

    def test_empty_ticker(self):
        with pytest.raises(ToolError, match="cannot be empty"):
            get_credit_ratings("")

    @patch("wrds_mcp.tools.ratings.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.ratings.get_wrds_connection")
    def test_query_failure(self, mock_get_conn, mock_resolve):
        mock_resolve.return_value = "001690"
        conn = MagicMock()
        conn.raw_sql.side_effect = Exception("Timeout")
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="WRDS query failed"):
            get_credit_ratings("AAPL")


class TestGetRatingsHistory:
    """Tests for get_ratings_history tool."""

    @patch("wrds_mcp.tools.ratings.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.ratings.get_wrds_connection")
    def test_happy_path(self, mock_get_conn, mock_resolve):
        mock_resolve.return_value = "001690"
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame({
            "gvkey": ["001690"] * 3,
            "datadate": pd.to_datetime(["2012-01-01", "2014-06-15", "2016-03-01"]),
            "splticrm": ["AA", "AA+", "AA+"],
        })
        mock_get_conn.return_value = conn

        result = get_ratings_history("AAPL", "2010-01-01", "2017-01-01")

        assert len(result) == 3
        assert result[0]["direction"] == "initial"
        assert result[0]["rating"] == "AA"
        assert result[1]["direction"] == "upgrade"
        assert result[1]["previous_rating"] == "AA"
        assert result[2]["direction"] == "affirmed"

    @patch("wrds_mcp.tools.ratings.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.ratings.get_wrds_connection")
    def test_downgrade_detected(self, mock_get_conn, mock_resolve):
        mock_resolve.return_value = "005000"
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame({
            "gvkey": ["005000"] * 2,
            "datadate": pd.to_datetime(["2013-01-01", "2015-06-01"]),
            "splticrm": ["BBB+", "BBB"],
        })
        mock_get_conn.return_value = conn

        result = get_ratings_history("XYZ", "2010-01-01", "2017-01-01")

        assert result[1]["direction"] == "downgrade"
        assert result[1]["previous_rating"] == "BBB+"

    @patch("wrds_mcp.tools.ratings.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.ratings.get_wrds_connection")
    def test_empty_results(self, mock_get_conn, mock_resolve):
        mock_resolve.return_value = "001690"
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame()
        mock_get_conn.return_value = conn

        result = get_ratings_history("AAPL", "2018-01-01", "2020-01-01")

        assert "message" in result[0]

    @patch("wrds_mcp.tools.ratings.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.ratings.get_wrds_connection")
    def test_ticker_not_found(self, mock_get_conn, mock_resolve):
        mock_resolve.return_value = None
        mock_get_conn.return_value = MagicMock()

        with pytest.raises(ToolError, match="not found in Compustat"):
            get_ratings_history("ZZZZ", "2010-01-01", "2017-01-01")

    def test_invalid_dates(self):
        with pytest.raises(ToolError, match="Expected format"):
            get_ratings_history("AAPL", "bad-date", "2017-01-01")

    def test_date_range_inverted(self):
        with pytest.raises(ToolError, match="must be before"):
            get_ratings_history("AAPL", "2017-01-01", "2010-01-01")

    @patch("wrds_mcp.tools.ratings.resolve_ticker_to_gvkey")
    @patch("wrds_mcp.tools.ratings.get_wrds_connection")
    def test_query_failure(self, mock_get_conn, mock_resolve):
        mock_resolve.return_value = "001690"
        conn = MagicMock()
        conn.raw_sql.side_effect = Exception("Network error")
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="WRDS query failed"):
            get_ratings_history("AAPL", "2010-01-01", "2017-01-01")
