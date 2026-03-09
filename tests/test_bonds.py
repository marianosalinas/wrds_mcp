"""Tests for bond tools."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastmcp.exceptions import ToolError

from wrds_mcp.tools.bonds import (
    get_bond_covenants,
    get_bond_price_history,
    get_bond_returns,
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


class TestGetBondPriceHistory:
    """Tests for get_bond_price_history tool."""

    @patch("wrds_mcp.tools.bonds._should_use_raw_trace", return_value=False)
    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_happy_path_enhanced(self, mock_get_conn, mock_raw_trace):
        df = pd.DataFrame({
            "cusip": ["037833AK6"] * 3,
            "date": pd.to_datetime(["2024-01-15", "2024-01-16", "2024-01-17"]),
            "avg_price": [99.50, 99.60, 99.45],
            "avg_yield": [4.52, 4.51, 4.53],
            "total_volume": [1500000, 2750000, 1200000],
            "num_trades": [5, 8, 3],
        })
        conn = MagicMock()
        conn.raw_sql.return_value = df
        mock_get_conn.return_value = conn

        result = get_bond_price_history("AAPL", "2024-01-15", "2024-01-17")

        assert isinstance(result, list)
        assert len(result) == 3
        assert result[0]["cusip"] == "037833AK6"
        assert result[0]["avg_price"] == 99.50
        assert result[0]["source"] == "wrdsapps_bondret.trace_enhanced_clean"

    @patch("wrds_mcp.tools.bonds._should_use_raw_trace", return_value=True)
    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_happy_path_raw_trace(self, mock_get_conn, mock_raw_trace):
        df = pd.DataFrame({
            "cusip": ["037833AK6"],
            "date": pd.to_datetime(["2025-12-01"]),
            "avg_price": [98.75],
            "avg_yield": [4.80],
            "total_volume": [500000],
            "num_trades": [2],
        })
        conn = MagicMock()
        conn.raw_sql.return_value = df
        mock_get_conn.return_value = conn

        result = get_bond_price_history("AAPL", "2025-11-01", "2025-12-31")

        assert len(result) == 1
        assert "trace.trace" in result[0]["source"]

    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_empty_results(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame()
        mock_get_conn.return_value = conn

        result = get_bond_price_history("ZZZZ", "2024-01-01", "2024-06-30")

        assert len(result) == 1
        assert "message" in result[0]
        assert "No bond price data" in result[0]["message"]
        assert "source" in result[0]

    def test_empty_ticker(self):
        with pytest.raises(ToolError, match="cannot be empty"):
            get_bond_price_history("", "2024-01-01", "2024-06-30")

    def test_invalid_ticker(self):
        with pytest.raises(ToolError, match="Invalid ticker"):
            get_bond_price_history("AAPL@#$", "2024-01-01", "2024-06-30")

    def test_invalid_date_format(self):
        with pytest.raises(ToolError, match="Expected format"):
            get_bond_price_history("AAPL", "01-15-2024", "2024-06-30")

    def test_date_range_inverted(self):
        with pytest.raises(ToolError, match="must be before"):
            get_bond_price_history("AAPL", "2024-06-30", "2024-01-01")

    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_query_failure(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.side_effect = Exception("Query timeout")
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="WRDS query failed"):
            get_bond_price_history("AAPL", "2024-01-01", "2024-06-30")

    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_connection_failure(self, mock_get_conn):
        mock_get_conn.side_effect = ConnectionError("Connection refused")

        with pytest.raises(ConnectionError):
            get_bond_price_history("AAPL", "2024-01-01", "2024-06-30")


class TestGetBondReturns:
    """Tests for get_bond_returns tool."""

    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_happy_path(self, mock_get_conn):
        df = pd.DataFrame({
            "cusip": ["037833AK6"] * 3,
            "date": pd.to_datetime(["2024-01-31", "2024-02-29", "2024-03-31"]),
            "bond_ret": [0.005, -0.002, 0.008],
            "tmt_yld": [4.50, 4.55, 4.48],
            "treasury_yield": [4.10, 4.12, 4.08],
            "credit_spread": [0.40, 0.43, 0.40],
            "oas": [45.0, 48.0, 44.0],
            "duration": [5.2, 5.1, 5.0],
            "price": [99.50, 99.30, 99.80],
            "amount_outstanding": [2000000000.0] * 3,
            "sp_rating": ["AA+"] * 3,
            "moody_rating": ["Aa1"] * 3,
            "fitch_rating": ["AA+"] * 3,
            "rating_cat": ["IG"] * 3,
            "rating_class": ["AA"] * 3,
        })
        conn = MagicMock()
        conn.raw_sql.return_value = df
        mock_get_conn.return_value = conn

        result = get_bond_returns("AAPL", "2024-01-01", "2024-03-31")

        assert isinstance(result, list)
        assert len(result) == 3
        assert result[0]["cusip"] == "037833AK6"
        assert result[0]["bond_ret"] == 0.005
        assert result[0]["sp_rating"] == "AA+"
        assert result[0]["duration"] == 5.2
        conn.raw_sql.assert_called_once()

    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_empty_results(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame()
        mock_get_conn.return_value = conn

        result = get_bond_returns("ZZZZ", "2024-01-01", "2024-06-30")

        assert len(result) == 1
        assert "message" in result[0]
        assert "No bond return data" in result[0]["message"]

    def test_empty_ticker(self):
        with pytest.raises(ToolError, match="cannot be empty"):
            get_bond_returns("", "2024-01-01", "2024-06-30")

    def test_invalid_ticker(self):
        with pytest.raises(ToolError, match="Invalid ticker"):
            get_bond_returns("AAPL@#$", "2024-01-01", "2024-06-30")

    def test_invalid_date_format(self):
        with pytest.raises(ToolError, match="Expected format"):
            get_bond_returns("AAPL", "01-01-2024", "2024-06-30")

    def test_date_range_inverted(self):
        with pytest.raises(ToolError, match="must be before"):
            get_bond_returns("AAPL", "2024-06-30", "2024-01-01")

    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_query_failure(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.side_effect = Exception("Query timeout")
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="WRDS query failed"):
            get_bond_returns("AAPL", "2024-01-01", "2024-06-30")

    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_connection_failure(self, mock_get_conn):
        mock_get_conn.side_effect = ConnectionError("Connection refused")

        with pytest.raises(ConnectionError):
            get_bond_returns("AAPL", "2024-01-01", "2024-06-30")

    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_nan_handling(self, mock_get_conn):
        df = pd.DataFrame({
            "cusip": ["037833AK6"],
            "date": pd.to_datetime(["2024-01-31"]),
            "bond_ret": [float("nan")],
            "tmt_yld": [4.50],
            "treasury_yield": [float("nan")],
            "credit_spread": [float("nan")],
            "oas": [float("nan")],
            "duration": [5.2],
            "price": [99.50],
            "amount_outstanding": [2e9],
            "sp_rating": [None],
            "moody_rating": ["Aa1"],
            "fitch_rating": [None],
            "rating_cat": ["IG"],
            "rating_class": ["AA"],
        })
        conn = MagicMock()
        conn.raw_sql.return_value = df
        mock_get_conn.return_value = conn

        result = get_bond_returns("AAPL", "2024-01-01", "2024-01-31")

        assert result[0]["bond_ret"] is None
        assert result[0]["sp_rating"] is None


class TestGetBondCovenants:
    """Tests for get_bond_covenants tool."""

    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_happy_path(self, mock_get_conn):
        bonds_df = pd.DataFrame({
            "complete_cusip": ["037833AK6", "037833AL4"],
            "coupon": [2.25, 3.05],
            "maturity": pd.to_datetime(["2026-05-01", "2029-02-15"]),
            "issuer_id": [500, 500],
        })
        cov_df = pd.DataFrame({
            "complete_cusip": ["037833AK6"],
            "covenant_id": [1],
            "cross_default": ["Y"],
            "cross_acceleration": ["N"],
            "change_control_put_provisions": ["Y"],
            "rating_decline_trigger_put": ["N"],
            "negative_pledge_covenant": ["Y"],
            "subsidiary_guarantee": ["N"],
            "liens_limitation": ["Y"],
            "restricted_payments_limitation": ["Y"],
            "consolidation_merger": ["Y"],
            "sale_assets": ["Y"],
            "senior_debt_issuance": ["N"],
            "subordinated_debt_issuance": ["N"],
            "stock_transfer_sale_disposal": ["N"],
        })
        call_df = pd.DataFrame({
            "complete_cusip": ["037833AK6"],
            "call_date": pd.to_datetime(["2025-05-01"]),
            "call_price": [100.0],
        })
        put_df = pd.DataFrame(columns=["complete_cusip", "put_date", "put_price"])
        sink_df = pd.DataFrame(columns=["complete_cusip", "sinking_fund_date", "sinking_fund_price", "sinking_fund_amount"])

        conn = MagicMock()
        conn.raw_sql.side_effect = [bonds_df, cov_df, call_df, put_df, sink_df]
        mock_get_conn.return_value = conn

        result = get_bond_covenants("AAPL")

        assert isinstance(result, dict)
        assert result["ticker"] == "AAPL"
        assert result["total_bonds"] == 2
        assert result["bonds_with_covenants"] == 1
        assert result["bonds_with_calls"] == 1
        assert result["bonds_with_puts"] == 0
        assert len(result["bonds"]) == 2

        # First bond has covenants
        bond1 = result["bonds"][0]
        assert bond1["cusip"] == "037833AK6"
        assert bond1["coupon"] == 2.25
        assert bond1["covenants"]["cross_default"] == "Y"
        assert bond1["covenants"]["negative_pledge"] == "Y"
        assert len(bond1["call_schedule"]) == 1

        # Second bond has no covenants
        bond2 = result["bonds"][1]
        assert bond2["covenants"] is None
        assert bond2["call_schedule"] == []

    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_no_bonds_found(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame()
        mock_get_conn.return_value = conn

        result = get_bond_covenants("ZZZZ")

        assert isinstance(result, dict)
        assert result["ticker"] == "ZZZZ"
        assert "No bonds found" in result["message"]
        assert result["bonds"] == []

    def test_empty_ticker(self):
        with pytest.raises(ToolError, match="cannot be empty"):
            get_bond_covenants("")

    def test_invalid_ticker(self):
        with pytest.raises(ToolError, match="Invalid ticker"):
            get_bond_covenants("AAPL@#$")

    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_initial_query_failure(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.side_effect = Exception("Query timeout")
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="WRDS query failed"):
            get_bond_covenants("AAPL")

    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_covenant_query_failure(self, mock_get_conn):
        bonds_df = pd.DataFrame({
            "complete_cusip": ["037833AK6"],
            "coupon": [2.25],
            "maturity": pd.to_datetime(["2026-05-01"]),
            "issuer_id": [500],
        })
        conn = MagicMock()
        # First call (cusip query) succeeds, second (covenant query) fails
        conn.raw_sql.side_effect = [bonds_df, Exception("Covenant query failed")]
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="WRDS covenant query failed"):
            get_bond_covenants("AAPL")

    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_connection_failure(self, mock_get_conn):
        mock_get_conn.side_effect = ConnectionError("Connection refused")

        with pytest.raises(ConnectionError):
            get_bond_covenants("AAPL")

    @patch("wrds_mcp.tools.bonds.get_wrds_connection")
    def test_bond_with_sinking_fund(self, mock_get_conn):
        bonds_df = pd.DataFrame({
            "complete_cusip": ["037833AK6"],
            "coupon": [2.25],
            "maturity": pd.to_datetime(["2026-05-01"]),
            "issuer_id": [500],
        })
        cov_df = pd.DataFrame(columns=["complete_cusip", "covenant_id", "cross_default",
            "cross_acceleration", "change_control_put_provisions",
            "rating_decline_trigger_put", "negative_pledge_covenant",
            "subsidiary_guarantee", "liens_limitation",
            "restricted_payments_limitation", "consolidation_merger",
            "sale_assets", "senior_debt_issuance", "subordinated_debt_issuance",
            "stock_transfer_sale_disposal"])
        call_df = pd.DataFrame(columns=["complete_cusip", "call_date", "call_price"])
        put_df = pd.DataFrame(columns=["complete_cusip", "put_date", "put_price"])
        sink_df = pd.DataFrame({
            "complete_cusip": ["037833AK6"],
            "sinking_fund_date": pd.to_datetime(["2025-11-01"]),
            "sinking_fund_price": [100.0],
            "sinking_fund_amount": [500000.0],
        })

        conn = MagicMock()
        conn.raw_sql.side_effect = [bonds_df, cov_df, call_df, put_df, sink_df]
        mock_get_conn.return_value = conn

        result = get_bond_covenants("AAPL")

        assert result["bonds"][0]["has_sinking_fund"] is True
