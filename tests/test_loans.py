"""Tests for syndicated loan tools."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastmcp.exceptions import ToolError

from wrds_mcp.tools.loans import (
    get_loan_terms,
    get_loan_covenants,
)


class TestGetLoanTerms:
    """Tests for get_loan_terms tool."""

    @patch("wrds_mcp.tools.loans.get_wrds_connection")
    def test_happy_path(self, mock_get_conn):
        df = pd.DataFrame({
            "facility_id": [10001, 10002],
            "facility_type": ["Term Loan", "Revolver/Line"],
            "facility_amt": [500000000.0, 1000000000.0],
            "facility_start_date": pd.to_datetime(["2023-03-15", "2023-03-15"]),
            "facility_end_date": pd.to_datetime(["2028-03-15", "2026-03-15"]),
            "currency": ["USD", "USD"],
            "deal_active_date": pd.to_datetime(["2023-03-15", "2023-03-15"]),
            "borrowercompanyid": [12345, 12345],
            "borrower_name": ["Ford Motor Company", "Ford Motor Company"],
            "spread": [175.0, 125.0],
            "base_rate": ["SOFR", "SOFR"],
        })
        conn = MagicMock()
        conn.raw_sql.return_value = df
        mock_get_conn.return_value = conn

        result = get_loan_terms("F")

        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["facility_type"] == "Term Loan"
        assert result[0]["spread"] == 175.0
        assert result[1]["facility_type"] == "Revolver/Line"
        conn.raw_sql.assert_called_once()

    @patch("wrds_mcp.tools.loans.get_wrds_connection")
    def test_deduplication(self, mock_get_conn):
        # Same facility with duplicate pricing rows
        df = pd.DataFrame({
            "facility_id": [10001, 10001],
            "facility_type": ["Term Loan", "Term Loan"],
            "facility_amt": [500e6, 500e6],
            "facility_start_date": pd.to_datetime(["2023-03-15", "2023-03-15"]),
            "facility_end_date": pd.to_datetime(["2028-03-15", "2028-03-15"]),
            "currency": ["USD", "USD"],
            "deal_active_date": pd.to_datetime(["2023-03-15", "2023-03-15"]),
            "borrowercompanyid": [12345, 12345],
            "borrower_name": ["Ford Motor Company", "Ford Motor Company"],
            "spread": [175.0, 175.0],
            "base_rate": ["SOFR", "SOFR"],
        })
        conn = MagicMock()
        conn.raw_sql.return_value = df
        mock_get_conn.return_value = conn

        result = get_loan_terms("F")

        # Should be deduplicated to 1 row
        assert len(result) == 1

    @patch("wrds_mcp.tools.loans.get_wrds_connection")
    def test_empty_results(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame()
        mock_get_conn.return_value = conn

        result = get_loan_terms("ZZZZ")

        assert len(result) == 1
        assert "message" in result[0]
        assert "No syndicated loan data" in result[0]["message"]

    def test_empty_ticker(self):
        with pytest.raises(ToolError, match="cannot be empty"):
            get_loan_terms("")

    def test_invalid_ticker(self):
        with pytest.raises(ToolError, match="Invalid ticker"):
            get_loan_terms("F@#$")

    @patch("wrds_mcp.tools.loans.get_wrds_connection")
    def test_query_failure(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.side_effect = Exception("Connection lost")
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="WRDS DealScan query failed"):
            get_loan_terms("F")

    @patch("wrds_mcp.tools.loans.get_wrds_connection")
    def test_connection_failure(self, mock_get_conn):
        mock_get_conn.side_effect = ConnectionError("Connection refused")

        with pytest.raises(ConnectionError):
            get_loan_terms("F")

    @patch("wrds_mcp.tools.loans.get_wrds_connection")
    def test_ticker_case_insensitive(self, mock_get_conn):
        df = pd.DataFrame({
            "facility_id": [10001],
            "facility_type": ["Term Loan"],
            "facility_amt": [500e6],
            "facility_start_date": pd.to_datetime(["2023-03-15"]),
            "facility_end_date": pd.to_datetime(["2028-03-15"]),
            "currency": ["USD"],
            "deal_active_date": pd.to_datetime(["2023-03-15"]),
            "borrowercompanyid": [12345],
            "borrower_name": ["Ford Motor Company"],
            "spread": [175.0],
            "base_rate": ["SOFR"],
        })
        conn = MagicMock()
        conn.raw_sql.return_value = df
        mock_get_conn.return_value = conn

        result = get_loan_terms("f")

        assert len(result) == 1
        # validate_ticker uppercases, so the param passed to SQL should be uppercased
        call_params = conn.raw_sql.call_args[1]["params"]
        assert call_params["ticker"] == "F"


class TestGetLoanCovenants:
    """Tests for get_loan_covenants tool."""

    @patch("wrds_mcp.tools.loans.get_wrds_connection")
    def test_happy_path_financial_covenants(self, mock_get_conn):
        fin_df = pd.DataFrame({
            "packageid": [100, 100],
            "covenant_type": ["Max Debt/EBITDA", "Min Interest Coverage"],
            "initial_ratio": [4.5, 2.0],
            "initial_amount": [float("nan"), float("nan")],
            "deal_active_date": pd.to_datetime(["2023-03-15", "2023-03-15"]),
        })
        nw_df = pd.DataFrame()  # No net worth covenants
        conn = MagicMock()
        conn.raw_sql.side_effect = [fin_df, nw_df]
        mock_get_conn.return_value = conn

        result = get_loan_covenants("F")

        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["covenant_category"] == "financial"
        assert result[0]["covenant_type"] == "Max Debt/EBITDA"
        assert result[0]["initial_ratio"] == 4.5

    @patch("wrds_mcp.tools.loans.get_wrds_connection")
    def test_happy_path_net_worth_covenants(self, mock_get_conn):
        fin_df = pd.DataFrame()
        nw_df = pd.DataFrame({
            "packageid": [100],
            "covenant_type": ["Min Net Worth"],
            "initial_amount": [1000000000.0],
            "deal_active_date": pd.to_datetime(["2023-03-15"]),
        })
        conn = MagicMock()
        conn.raw_sql.side_effect = [fin_df, nw_df]
        mock_get_conn.return_value = conn

        result = get_loan_covenants("F")

        assert len(result) == 1
        assert result[0]["covenant_category"] == "net_worth"
        assert result[0]["initial_amount"] == 1000000000.0

    @patch("wrds_mcp.tools.loans.get_wrds_connection")
    def test_both_covenant_types(self, mock_get_conn):
        fin_df = pd.DataFrame({
            "packageid": [100],
            "covenant_type": ["Max Debt/EBITDA"],
            "initial_ratio": [4.5],
            "initial_amount": [float("nan")],
            "deal_active_date": pd.to_datetime(["2023-03-15"]),
        })
        nw_df = pd.DataFrame({
            "packageid": [100],
            "covenant_type": ["Min Net Worth"],
            "initial_amount": [1e9],
            "deal_active_date": pd.to_datetime(["2023-03-15"]),
        })
        conn = MagicMock()
        conn.raw_sql.side_effect = [fin_df, nw_df]
        mock_get_conn.return_value = conn

        result = get_loan_covenants("F")

        assert len(result) == 2
        categories = [r["covenant_category"] for r in result]
        assert "financial" in categories
        assert "net_worth" in categories

    @patch("wrds_mcp.tools.loans.get_wrds_connection")
    def test_empty_results(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.side_effect = [pd.DataFrame(), pd.DataFrame()]
        mock_get_conn.return_value = conn

        result = get_loan_covenants("ZZZZ")

        assert len(result) == 1
        assert "message" in result[0]
        assert "No loan covenants found" in result[0]["message"]

    def test_empty_ticker(self):
        with pytest.raises(ToolError, match="cannot be empty"):
            get_loan_covenants("")

    def test_invalid_ticker(self):
        with pytest.raises(ToolError, match="Invalid ticker"):
            get_loan_covenants("F@#$")

    @patch("wrds_mcp.tools.loans.get_wrds_connection")
    def test_query_failure(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.side_effect = Exception("Connection lost")
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="WRDS DealScan query failed"):
            get_loan_covenants("F")

    @patch("wrds_mcp.tools.loans.get_wrds_connection")
    def test_connection_failure(self, mock_get_conn):
        mock_get_conn.side_effect = ConnectionError("Connection refused")

        with pytest.raises(ConnectionError):
            get_loan_covenants("F")

    @patch("wrds_mcp.tools.loans.get_wrds_connection")
    def test_null_initial_ratio_handled(self, mock_get_conn):
        fin_df = pd.DataFrame({
            "packageid": [100],
            "covenant_type": ["Max Debt/EBITDA"],
            "initial_ratio": [float("nan")],
            "initial_amount": [float("nan")],
            "deal_active_date": pd.to_datetime(["2023-03-15"]),
        })
        nw_df = pd.DataFrame()
        conn = MagicMock()
        conn.raw_sql.side_effect = [fin_df, nw_df]
        mock_get_conn.return_value = conn

        result = get_loan_covenants("F")

        assert "initial_ratio" not in result[0]  # NaN ratios are omitted
