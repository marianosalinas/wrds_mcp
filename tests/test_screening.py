"""Tests for screening tools."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastmcp.exceptions import ToolError

import wrds_mcp.tools.screening as screening_mod
from wrds_mcp.tools.screening import (
    RATING_CAT_TO_NUM_SQL,
    RATING_TO_NUM,
    SECTOR_SIC_RANGES,
    _build_sic_filter,
    _detect_latest_full_month,
    _validate_rating,
    get_market_benchmarks,
    get_relative_value,
    screen_bonds,
    screen_issuers,
)


# Reset module-level cache between tests
@pytest.fixture(autouse=True)
def reset_latest_month_cache():
    screening_mod._latest_full_month = None
    yield
    screening_mod._latest_full_month = None


# --- Helper function tests ---


class TestValidateRating:
    def test_valid_ratings(self):
        assert _validate_rating("AAA") == 1
        assert _validate_rating("BB") == 12
        assert _validate_rating("CCC-") == 19
        assert _validate_rating("D") == 22

    def test_case_insensitive(self):
        assert _validate_rating("bbb+") == 8
        assert _validate_rating("aa-") == 4

    def test_strips_whitespace(self):
        assert _validate_rating("  A+  ") == 5

    def test_invalid_rating(self):
        with pytest.raises(ToolError, match="Invalid rating"):
            _validate_rating("XYZ")

    def test_empty_rating(self):
        with pytest.raises(ToolError, match="Invalid rating"):
            _validate_rating("")


class TestBuildSicFilter:
    def test_single_range(self):
        params = {}
        result = _build_sic_filter([(1300, 1399)], params, col="f.sich")
        assert "f.sich BETWEEN :sic_lo_0 AND :sic_hi_0" in result
        assert params["sic_lo_0"] == 1300
        assert params["sic_hi_0"] == 1399

    def test_multiple_ranges(self):
        params = {}
        result = _build_sic_filter([(1300, 1399), (2911, 2911)], params)
        assert "sic_lo_0" in result
        assert "sic_lo_1" in result
        assert " OR " in result
        assert result.startswith("(")
        assert result.endswith(")")

    def test_custom_column(self):
        params = {}
        result = _build_sic_filter([(1300, 1399)], params, col="fin.sic_code")
        assert "fin.sic_code BETWEEN" in result


class TestDetectLatestFullMonth:
    def test_returns_date(self):
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame({"date": ["2025-03-31"], "n": [800]})

        result = _detect_latest_full_month(conn)
        assert result == "2025-03-31"

    def test_caches_result(self):
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame({"date": ["2025-03-31"], "n": [800]})

        _detect_latest_full_month(conn)
        _detect_latest_full_month(conn)

        # Only called once due to cache
        conn.raw_sql.assert_called_once()

    def test_empty_raises(self):
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame()

        with pytest.raises(ToolError, match="Could not detect"):
            _detect_latest_full_month(conn)


class TestRatingCatToNumSql:
    def test_sql_has_all_broad_categories(self):
        for cat in ["AAA", "AA", "A", "BBB", "BB", "B", "CCC", "CC", "C", "D"]:
            assert f"WHEN '{cat}' THEN" in RATING_CAT_TO_NUM_SQL

    def test_sql_else_null(self):
        assert "ELSE NULL" in RATING_CAT_TO_NUM_SQL


# --- Issuer mock data helpers ---

def _make_issuer_df(**overrides):
    """Return a realistic issuer result DataFrame."""
    defaults = {
        "ticker": ["XOM"],
        "company_name": ["Exxon Mobil Corp"],
        "sp_rating": ["A+"],
        "moody_rating": ["Aa2"],
        "fitch_rating": [None],
        "rating_class": ["0.IG"],
        "rating_cat": ["A"],
        "sic_code": [1311],
        "market_cap": [400000.0],
        "revenue": [350000.0],
        "ebitda": [55000.0],
        "total_debt": [40000.0],
        "net_debt": [30000.0],
        "leverage": [0.73],
        "interest_coverage": [20.0],
        "financials_date": ["2024-12-31"],
        "equity_return_1mo": [0.02],
        "equity_return_3mo": [0.05],
        "equity_return_6mo": [0.08],
        "equity_return_12mo": [0.15],
    }
    defaults.update(overrides)
    return pd.DataFrame(defaults)


def _make_bond_df(**overrides):
    """Return a realistic bond screening result DataFrame."""
    defaults = {
        "cusip": ["345370CX5"],
        "ticker": ["F"],
        "coupon": [4.346],
        "maturity": ["2026-12-08"],
        "security_level": ["SU"],
        "offering_amt": [1250.0],
        "amount_outstanding": [1250.0],
        "sp_rating": ["BB+"],
        "moody_rating": ["Ba1"],
        "rating_class": ["1.HY"],
        "rating_cat": ["BB"],
        "spread_bps": [150.3],
        "yield_pct": [5.812],
        "price": [98.5],
        "duration": [2.35],
        "return_1mo": [0.008],
        "return_3mo": [0.025],
    }
    defaults.update(overrides)
    return pd.DataFrame(defaults)


def _mock_conn_with_latest_month(mock_get_conn, result_df):
    """Set up mock to return latest month detection + result query."""
    latest_month_df = pd.DataFrame({"date": ["2025-03-31"], "n": [800]})
    conn = MagicMock()
    conn.raw_sql.side_effect = [latest_month_df, result_df]
    mock_get_conn.return_value = conn
    return conn


# --- screen_issuers tests ---


class TestScreenIssuers:
    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_happy_path_no_filters(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_issuer_df())

        result = screen_issuers()

        assert result["as_of_date"] == "2025-03-31"
        assert result["result_count"] == 1
        assert result["filters_applied"] == {}
        assert result["issuers"][0]["ticker"] == "XOM"
        assert result["issuers"][0]["market_cap"] == 400000.0

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_rating_class_filter(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_issuer_df())

        result = screen_issuers(rating_class="HY")

        assert result["filters_applied"]["rating_class"] == "HY"
        query = conn.raw_sql.call_args_list[1][0][0]
        assert "rating_class = :rating_class" in query
        params = conn.raw_sql.call_args_list[1][1]["params"]
        assert params["rating_class"] == "1.HY"

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_rating_class_ig(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_issuer_df())

        result = screen_issuers(rating_class="IG")

        params = conn.raw_sql.call_args_list[1][1]["params"]
        assert params["rating_class"] == "0.IG"

    def test_invalid_rating_class(self):
        # rating_class validation happens before DB call, but we need to mock
        # get_wrds_connection to avoid real connection
        with patch("wrds_mcp.tools.screening.get_wrds_connection") as mock_get_conn:
            latest_month_df = pd.DataFrame({"date": ["2025-03-31"], "n": [800]})
            conn = MagicMock()
            conn.raw_sql.return_value = latest_month_df
            mock_get_conn.return_value = conn

            with pytest.raises(ToolError, match="rating_class must be 'HY' or 'IG'"):
                screen_issuers(rating_class="XX")

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_min_rating_filter(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_issuer_df())

        result = screen_issuers(min_rating="BBB+")

        assert result["filters_applied"]["min_rating"] == "BBB+"
        params = conn.raw_sql.call_args_list[1][1]["params"]
        assert params["min_rating_num"] == 8  # BBB+ = 8

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_max_rating_filter(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_issuer_df())

        result = screen_issuers(max_rating="BB-")

        assert result["filters_applied"]["max_rating"] == "BB-"
        params = conn.raw_sql.call_args_list[1][1]["params"]
        assert params["max_rating_num"] == 13  # BB- = 13

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_rating_range_crossover(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_issuer_df())

        result = screen_issuers(min_rating="BBB+", max_rating="BB-")

        query = conn.raw_sql.call_args_list[1][0][0]
        # Both rating CASE expressions should be in query
        assert "CASE b.rating_cat" in query
        assert "min_rating_num" in str(conn.raw_sql.call_args_list[1][1]["params"])
        assert "max_rating_num" in str(conn.raw_sql.call_args_list[1][1]["params"])

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_uses_case_expression_not_int_cast(self, mock_get_conn):
        """Verify we use CASE expression, not rating_cat::int."""
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_issuer_df())

        screen_issuers(min_rating="BB")

        query = conn.raw_sql.call_args_list[1][0][0]
        assert "rating_cat::int" not in query
        assert "CASE b.rating_cat" in query

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_sector_filter(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_issuer_df())

        result = screen_issuers(sector="Energy")

        assert result["filters_applied"]["sector"] == "Energy"
        params = conn.raw_sql.call_args_list[1][1]["params"]
        # Energy SIC ranges should be in params
        assert params["sic_lo_0"] == 1300

    def test_invalid_sector(self):
        with patch("wrds_mcp.tools.screening.get_wrds_connection") as mock_get_conn:
            latest_month_df = pd.DataFrame({"date": ["2025-03-31"], "n": [800]})
            conn = MagicMock()
            conn.raw_sql.return_value = latest_month_df
            mock_get_conn.return_value = conn

            with pytest.raises(ToolError, match="Unknown sector"):
                screen_issuers(sector="Crypto")

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_market_cap_filter(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_issuer_df())

        result = screen_issuers(min_market_cap=5000, max_market_cap=50000)

        assert result["filters_applied"]["min_market_cap"] == 5000
        assert result["filters_applied"]["max_market_cap"] == 50000
        params = conn.raw_sql.call_args_list[1][1]["params"]
        assert params["min_mktcap"] == 5000
        assert params["max_mktcap"] == 50000

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_ebitda_filter(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_issuer_df())

        result = screen_issuers(min_ebitda=1000)

        assert result["filters_applied"]["min_ebitda"] == 1000

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_leverage_filters(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_issuer_df())

        result = screen_issuers(min_leverage=2.0, max_leverage=5.0)

        assert result["filters_applied"]["min_leverage"] == 2.0
        assert result["filters_applied"]["max_leverage"] == 5.0
        query = conn.raw_sql.call_args_list[1][0][0]
        assert "leverage IS NOT NULL" in query

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_empty_results(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, pd.DataFrame())

        result = screen_issuers(rating_class="HY")

        assert result["result_count"] == 0
        assert "message" in result["issuers"][0]
        assert "No issuers match" in result["issuers"][0]["message"]

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_query_failure(self, mock_get_conn):
        latest_month_df = pd.DataFrame({"date": ["2025-03-31"], "n": [800]})
        conn = MagicMock()
        conn.raw_sql.side_effect = [latest_month_df, Exception("timeout")]
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="Screening query failed"):
            screen_issuers()

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_sort_by_market_cap(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_issuer_df())

        screen_issuers(sort_by="market_cap")

        query = conn.raw_sql.call_args_list[1][0][0]
        assert "market_cap DESC" in query

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_sort_by_sp_rating(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_issuer_df())

        screen_issuers(sort_by="sp_rating")

        query = conn.raw_sql.call_args_list[1][0][0]
        assert "rating_num ASC" in query

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_invalid_sort_defaults_to_market_cap(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_issuer_df())

        screen_issuers(sort_by="invalid_sort")

        query = conn.raw_sql.call_args_list[1][0][0]
        assert "market_cap DESC" in query

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_limit_param(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_issuer_df())

        screen_issuers(limit=25)

        params = conn.raw_sql.call_args_list[1][1]["params"]
        assert params["limit"] == 25

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_combined_filters(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_issuer_df())

        result = screen_issuers(
            rating_class="HY",
            sector="Energy",
            min_market_cap=5000,
            max_leverage=5.0,
        )

        assert result["filters_applied"]["rating_class"] == "HY"
        assert result["filters_applied"]["sector"] == "Energy"
        assert result["filters_applied"]["min_market_cap"] == 5000
        assert result["filters_applied"]["max_leverage"] == 5.0

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_multiple_issuers(self, mock_get_conn):
        df = _make_issuer_df(
            ticker=["XOM", "CVX"],
            company_name=["Exxon", "Chevron"],
            market_cap=[400000.0, 300000.0],
            sp_rating=["A+", "AA-"],
            moody_rating=["Aa2", "Aa3"],
            fitch_rating=[None, None],
            rating_class=["0.IG", "0.IG"],
            rating_cat=["A", "AA"],
            sic_code=[1311, 1311],
            revenue=[350000.0, 200000.0],
            ebitda=[55000.0, 40000.0],
            total_debt=[40000.0, 30000.0],
            net_debt=[30000.0, 20000.0],
            leverage=[0.73, 0.75],
            interest_coverage=[20.0, 18.0],
            financials_date=["2024-12-31", "2024-12-31"],
            equity_return_1mo=[0.02, 0.01],
            equity_return_3mo=[0.05, 0.03],
            equity_return_6mo=[0.08, 0.06],
            equity_return_12mo=[0.15, 0.12],
        )
        conn = _mock_conn_with_latest_month(mock_get_conn, df)

        result = screen_issuers()

        assert result["result_count"] == 2


class TestScreenBonds:
    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_happy_path_no_filters(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_bond_df())

        result = screen_bonds()

        assert result["as_of_date"] == "2025-03-31"
        assert result["result_count"] == 1
        assert result["filters_applied"] == {}
        assert result["bonds"][0]["ticker"] == "F"
        assert result["bonds"][0]["coupon"] == 4.346

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_ticker_filter(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_bond_df())

        result = screen_bonds(ticker="F")

        assert result["filters_applied"]["ticker"] == "F"
        params = conn.raw_sql.call_args_list[1][1]["params"]
        assert params["ticker"] == "F"

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_ticker_case_insensitive(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_bond_df())

        result = screen_bonds(ticker="f")

        params = conn.raw_sql.call_args_list[1][1]["params"]
        assert params["ticker"] == "F"

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_rating_class_filter(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_bond_df())

        result = screen_bonds(rating_class="HY")

        assert result["filters_applied"]["rating_class"] == "HY"
        params = conn.raw_sql.call_args_list[1][1]["params"]
        assert params["rating_class"] == "1.HY"

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_min_rating_uses_case_expression(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_bond_df())

        screen_bonds(min_rating="BB")

        query = conn.raw_sql.call_args_list[1][0][0]
        assert "rating_cat::int" not in query
        assert "CASE b.rating_cat" in query

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_max_rating_filter(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_bond_df())

        result = screen_bonds(max_rating="BB-")

        assert result["filters_applied"]["max_rating"] == "BB-"
        params = conn.raw_sql.call_args_list[1][1]["params"]
        assert params["max_rating_num"] == 13

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_security_level_filter(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_bond_df())

        result = screen_bonds(security_level="SU")

        assert result["filters_applied"]["security_level"] == "SU"

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_amount_outstanding_filter(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_bond_df())

        result = screen_bonds(min_amount_outstanding=500)

        assert result["filters_applied"]["min_amount_outstanding"] == 500

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_coupon_filters(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_bond_df())

        result = screen_bonds(min_coupon=3.0, max_coupon=7.0)

        assert result["filters_applied"]["min_coupon"] == 3.0
        assert result["filters_applied"]["max_coupon"] == 7.0

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_maturity_filters(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_bond_df())

        result = screen_bonds(maturity_after="2026-01-01", maturity_before="2030-12-31")

        assert result["filters_applied"]["maturity_after"] == "2026-01-01"
        assert result["filters_applied"]["maturity_before"] == "2030-12-31"

    def test_maturity_invalid_date(self):
        with patch("wrds_mcp.tools.screening.get_wrds_connection") as mock_get_conn:
            latest_month_df = pd.DataFrame({"date": ["2025-03-31"], "n": [800]})
            conn = MagicMock()
            conn.raw_sql.return_value = latest_month_df
            mock_get_conn.return_value = conn

            with pytest.raises(ToolError, match="Expected format"):
                screen_bonds(maturity_after="01-01-2026")

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_spread_filters(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_bond_df())

        result = screen_bonds(min_spread=100, max_spread=500)

        assert result["filters_applied"]["min_spread"] == 100
        assert result["filters_applied"]["max_spread"] == 500

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_sector_filter_adds_join(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_bond_df())

        result = screen_bonds(sector="Energy")

        assert result["filters_applied"]["sector"] == "Energy"
        query = conn.raw_sql.call_args_list[1][0][0]
        assert "comp.security" in query
        assert "comp.funda" in query

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_no_sector_skips_join(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_bond_df())

        screen_bonds()

        query = conn.raw_sql.call_args_list[1][0][0]
        assert "comp.funda" not in query

    def test_invalid_sector(self):
        with patch("wrds_mcp.tools.screening.get_wrds_connection") as mock_get_conn:
            latest_month_df = pd.DataFrame({"date": ["2025-03-31"], "n": [800]})
            conn = MagicMock()
            conn.raw_sql.return_value = latest_month_df
            mock_get_conn.return_value = conn

            with pytest.raises(ToolError, match="Unknown sector"):
                screen_bonds(sector="Metaverse")

    def test_invalid_rating_class(self):
        with patch("wrds_mcp.tools.screening.get_wrds_connection") as mock_get_conn:
            latest_month_df = pd.DataFrame({"date": ["2025-03-31"], "n": [800]})
            conn = MagicMock()
            conn.raw_sql.return_value = latest_month_df
            mock_get_conn.return_value = conn

            with pytest.raises(ToolError, match="rating_class must be"):
                screen_bonds(rating_class="XX")

    def test_invalid_ticker(self):
        with patch("wrds_mcp.tools.screening.get_wrds_connection") as mock_get_conn:
            latest_month_df = pd.DataFrame({"date": ["2025-03-31"], "n": [800]})
            conn = MagicMock()
            conn.raw_sql.return_value = latest_month_df
            mock_get_conn.return_value = conn

            with pytest.raises(ToolError, match="Invalid ticker"):
                screen_bonds(ticker="F@#$")

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_empty_results(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, pd.DataFrame())

        result = screen_bonds(ticker="ZZZZ")

        assert result["result_count"] == 0
        assert "message" in result["bonds"][0]
        assert "No bonds match" in result["bonds"][0]["message"]

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_query_failure(self, mock_get_conn):
        latest_month_df = pd.DataFrame({"date": ["2025-03-31"], "n": [800]})
        conn = MagicMock()
        conn.raw_sql.side_effect = [latest_month_df, Exception("timeout")]
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="Bond screening query failed"):
            screen_bonds()

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_sort_by_spread(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_bond_df())

        screen_bonds(sort_by="spread")

        query = conn.raw_sql.call_args_list[1][0][0]
        assert "t_spread DESC" in query

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_sort_by_rating_uses_case(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_bond_df())

        screen_bonds(sort_by="rating")

        query = conn.raw_sql.call_args_list[1][0][0]
        # Sort should use CASE expression
        assert "CASE b.rating_cat" in query

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_invalid_sort_defaults_to_spread(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_bond_df())

        screen_bonds(sort_by="invalid")

        query = conn.raw_sql.call_args_list[1][0][0]
        assert "t_spread DESC" in query

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_limit_param(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_bond_df())

        screen_bonds(limit=50)

        params = conn.raw_sql.call_args_list[1][1]["params"]
        assert params["limit"] == 50

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_combined_filters(self, mock_get_conn):
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_bond_df())

        result = screen_bonds(
            rating_class="HY",
            min_amount_outstanding=300,
            min_coupon=3.0,
            maturity_after="2026-01-01",
            sort_by="spread",
            limit=50,
        )

        assert result["filters_applied"]["rating_class"] == "HY"
        assert result["filters_applied"]["min_amount_outstanding"] == 300
        assert result["filters_applied"]["min_coupon"] == 3.0
        assert result["filters_applied"]["maturity_after"] == "2026-01-01"

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_multiple_bonds(self, mock_get_conn):
        df = pd.DataFrame({
            "cusip": ["345370CX5", "345370CY3"],
            "ticker": ["F", "F"],
            "coupon": [4.346, 6.625],
            "maturity": ["2026-12-08", "2028-02-15"],
            "security_level": ["SU", "SU"],
            "offering_amt": [1250.0, 800.0],
            "amount_outstanding": [1250.0, 800.0],
            "sp_rating": ["BB+", "BB+"],
            "moody_rating": ["Ba1", "Ba1"],
            "rating_class": ["1.HY", "1.HY"],
            "rating_cat": ["BB", "BB"],
            "spread_bps": [150.3, 220.1],
            "yield_pct": [5.812, 6.543],
            "price": [98.5, 97.2],
            "duration": [2.35, 3.10],
            "return_1mo": [0.008, 0.005],
            "return_3mo": [0.025, 0.018],
        })
        conn = _mock_conn_with_latest_month(mock_get_conn, df)

        result = screen_bonds()

        assert result["result_count"] == 2

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_connection_failure(self, mock_get_conn):
        mock_get_conn.side_effect = ConnectionError("Connection refused")

        with pytest.raises(ConnectionError):
            screen_bonds()

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_excludes_abs_and_convertible(self, mock_get_conn):
        """Query should filter out asset-backed and convertible bonds."""
        conn = _mock_conn_with_latest_month(mock_get_conn, _make_bond_df())

        screen_bonds()

        query = conn.raw_sql.call_args_list[1][0][0]
        assert "asset_backed = 'N'" in query
        assert "convertible = 'N'" in query


# --- get_market_benchmarks tests ---

def _make_benchmark_df(**overrides):
    defaults = {
        "date": pd.to_datetime(["2025-01-31", "2025-02-28"]),
        "bond_count": [5000, 5100],
        "issuer_count": [600, 610],
        "avg_spread": [180.5, 175.3],
        "avg_yield": [5.5, 5.4],
        "avg_return": [0.008, 0.005],
        "avg_duration": [4.5, 4.4],
        "avg_price": [95.0, 95.5],
        "total_outstanding": [500000.0, 510000.0],
        "vw_spread": [190.2, 185.1],
        "vw_yield": [5.6, 5.5],
        "vw_return": [0.007, 0.004],
    }
    defaults.update(overrides)
    return pd.DataFrame(defaults)


class TestGetMarketBenchmarks:
    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_happy_path(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.return_value = _make_benchmark_df()
        mock_get_conn.return_value = conn

        result = get_market_benchmarks("2025-01-01", "2025-03-31")

        assert result["months"] == 2
        assert result["period"]["start"] == "2025-01-01"
        assert result["period"]["end"] == "2025-03-31"
        assert len(result["monthly_data"]) == 2
        assert "cumulative_vw_return" in result["summary"]

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_rating_class_filter(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.return_value = _make_benchmark_df()
        mock_get_conn.return_value = conn

        result = get_market_benchmarks("2025-01-01", "2025-03-31", rating_class="HY")

        assert result["filters_applied"]["rating_class"] == "HY"
        params = conn.raw_sql.call_args[1]["params"]
        assert params["rating_class"] == "1.HY"

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_rating_category_filter(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.return_value = _make_benchmark_df()
        mock_get_conn.return_value = conn

        result = get_market_benchmarks("2025-01-01", "2025-03-31", rating_category="BB")

        assert result["filters_applied"]["rating_category"] == "BB"
        params = conn.raw_sql.call_args[1]["params"]
        assert params["rating_cat"] == "BB"

    def test_invalid_rating_class(self):
        with pytest.raises(ToolError, match="rating_class must be"):
            get_market_benchmarks("2025-01-01", "2025-03-31", rating_class="XX")

    def test_invalid_rating_category(self):
        with pytest.raises(ToolError, match="Invalid rating_category"):
            get_market_benchmarks("2025-01-01", "2025-03-31", rating_category="XYZ")

    def test_invalid_date(self):
        with pytest.raises(ToolError, match="Expected format"):
            get_market_benchmarks("01-01-2025", "2025-03-31")

    def test_date_range_inverted(self):
        with pytest.raises(ToolError, match="must be before"):
            get_market_benchmarks("2025-06-30", "2025-01-01")

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_empty_results(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame()
        mock_get_conn.return_value = conn

        result = get_market_benchmarks("2030-01-01", "2030-12-31")

        assert "message" in result["monthly_data"][0]

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_query_failure(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.side_effect = Exception("timeout")
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="Benchmark query failed"):
            get_market_benchmarks("2025-01-01", "2025-03-31")

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_cumulative_return_compounding(self, mock_get_conn):
        conn = MagicMock()
        df = _make_benchmark_df(vw_return=[0.01, 0.02])
        conn.raw_sql.return_value = df
        mock_get_conn.return_value = conn

        result = get_market_benchmarks("2025-01-01", "2025-03-31")

        # (1.01)(1.02) - 1 = 0.0302
        last = result["monthly_data"][-1]
        assert abs(last["cumulative_vw_return"] - 0.0302) < 0.001


# --- get_relative_value tests ---

def _make_issuer_bonds_df():
    return pd.DataFrame({
        "cusip": ["345370CX5", "345370CY3"],
        "ticker": ["F", "F"],
        "coupon": [4.346, 6.625],
        "maturity": ["2026-12-08", "2028-02-15"],
        "security_level": ["SU", "SU"],
        "amount_outstanding": [1250.0, 800.0],
        "sp_rating": ["BB+", "BB+"],
        "moody_rating": ["Ba1", "Ba1"],
        "rating_cat": ["BB", "BB"],
        "rating_class": ["1.HY", "1.HY"],
        "spread_bps": [200.0, 250.0],
        "yield_pct": [5.8, 6.3],
        "price": [98.5, 97.2],
        "duration": [2.35, 3.10],
        "return_1mo": [0.008, 0.005],
    })


def _make_peer_stats_df():
    return pd.DataFrame({
        "rating_cat": ["BB"],
        "bond_count": [500],
        "issuer_count": [80],
        "avg_spread": [180.0],
        "spread_p25": [140.0],
        "spread_median": [175.0],
        "spread_p75": [220.0],
        "avg_yield": [5.5],
        "avg_duration": [4.0],
        "avg_price": [96.0],
        "avg_return_1mo": [0.006],
    })


class TestGetRelativeValue:
    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_happy_path(self, mock_get_conn):
        latest_month_df = pd.DataFrame({"date": ["2025-03-31"], "n": [800]})
        conn = MagicMock()
        conn.raw_sql.side_effect = [
            latest_month_df,
            _make_issuer_bonds_df(),
            _make_peer_stats_df(),
        ]
        mock_get_conn.return_value = conn

        result = get_relative_value("F")

        assert result["ticker"] == "F"
        assert result["bond_count"] == 2
        assert result["as_of_date"] == "2025-03-31"
        assert "peer_stats" in result
        assert "BB" in result["peer_stats"]
        # First bond: 200 - 180 = 20, so "fair" (not > 20)
        assert result["bonds"][0]["spread_vs_peers"] == 20.0
        # Second bond: 250 - 180 = 70, so "cheap"
        assert result["bonds"][1]["spread_vs_peers"] == 70.0
        assert result["bonds"][1]["relative_value"] == "cheap"

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_rich_bond(self, mock_get_conn):
        latest_month_df = pd.DataFrame({"date": ["2025-03-31"], "n": [800]})
        issuer_df = pd.DataFrame({
            "cusip": ["345370CX5"],
            "ticker": ["F"],
            "coupon": [4.346],
            "maturity": ["2026-12-08"],
            "security_level": ["SU"],
            "amount_outstanding": [1250.0],
            "sp_rating": ["BB+"],
            "moody_rating": ["Ba1"],
            "rating_cat": ["BB"],
            "rating_class": ["1.HY"],
            "spread_bps": [140.0],  # Tight spread
            "yield_pct": [5.0],
            "price": [100.5],
            "duration": [2.35],
            "return_1mo": [0.008],
        })
        conn = MagicMock()
        conn.raw_sql.side_effect = [latest_month_df, issuer_df, _make_peer_stats_df()]
        mock_get_conn.return_value = conn

        result = get_relative_value("F")

        # 140 - 180 = -40, so "rich"
        assert result["bonds"][0]["spread_vs_peers"] == -40.0
        assert result["bonds"][0]["relative_value"] == "rich"

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_empty_issuer(self, mock_get_conn):
        latest_month_df = pd.DataFrame({"date": ["2025-03-31"], "n": [800]})
        conn = MagicMock()
        conn.raw_sql.side_effect = [latest_month_df, pd.DataFrame()]
        mock_get_conn.return_value = conn

        result = get_relative_value("ZZZZ")

        assert "message" in result
        assert "No bonds found" in result["message"]

    def test_invalid_ticker(self):
        with pytest.raises(ToolError, match="Invalid ticker"):
            get_relative_value("F@#$")

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_query_failure(self, mock_get_conn):
        latest_month_df = pd.DataFrame({"date": ["2025-03-31"], "n": [800]})
        conn = MagicMock()
        conn.raw_sql.side_effect = [latest_month_df, Exception("timeout")]
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="Issuer query failed"):
            get_relative_value("F")

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_yield_vs_peers(self, mock_get_conn):
        latest_month_df = pd.DataFrame({"date": ["2025-03-31"], "n": [800]})
        conn = MagicMock()
        conn.raw_sql.side_effect = [
            latest_month_df,
            _make_issuer_bonds_df(),
            _make_peer_stats_df(),
        ]
        mock_get_conn.return_value = conn

        result = get_relative_value("F")

        # First bond yield_pct 5.8, peer avg 5.5 => +0.3
        assert result["bonds"][0]["yield_vs_peers"] == 0.3

    @patch("wrds_mcp.tools.screening.get_wrds_connection")
    def test_peer_stats_exclude_issuer(self, mock_get_conn):
        """Peer stats query should exclude the issuer itself."""
        latest_month_df = pd.DataFrame({"date": ["2025-03-31"], "n": [800]})
        conn = MagicMock()
        conn.raw_sql.side_effect = [
            latest_month_df,
            _make_issuer_bonds_df(),
            _make_peer_stats_df(),
        ]
        mock_get_conn.return_value = conn

        get_relative_value("F")

        # The peer stats query (3rd call, index 2) should have ticker exclusion
        peer_query = conn.raw_sql.call_args_list[2][0][0]
        assert "!= :ticker" in peer_query
