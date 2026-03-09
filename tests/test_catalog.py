"""Tests for data catalog discovery tools."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastmcp.exceptions import ToolError

from wrds_mcp.tools.catalog import (
    get_data_catalog,
    get_table_schema,
    resolve_identifier,
    _catalog_cache,
)


class TestGetDataCatalog:
    def setup_method(self):
        """Reset catalog cache before each test."""
        import wrds_mcp.tools.catalog as cat_mod
        cat_mod._catalog_cache = None

    @patch("wrds_mcp.tools.catalog.get_wrds_connection")
    @patch("wrds_mcp.tools.catalog._check_schema_exists", return_value=True)
    @patch("wrds_mcp.tools.catalog._query_date_range", return_value={
        "earliest": "2000-01-01", "latest": "2024-12-31", "row_count": 1000000,
    })
    def test_returns_catalog(self, mock_dates, mock_schema, mock_conn):
        result = get_data_catalog()

        assert "equity" in result
        assert "bonds" in result
        assert "ratings" in result
        assert "financials" in result
        assert "loans" in result
        assert "composite_tools" in result

    @patch("wrds_mcp.tools.catalog.get_wrds_connection")
    @patch("wrds_mcp.tools.catalog._check_schema_exists", return_value=True)
    @patch("wrds_mcp.tools.catalog._query_date_range", return_value={
        "earliest": "2000-01-01", "latest": "2024-12-31", "row_count": 1000000,
    })
    def test_caching(self, mock_dates, mock_schema, mock_conn):
        result1 = get_data_catalog()
        result2 = get_data_catalog()

        # Connection should only be called once (cached second time)
        mock_conn.assert_called_once()
        assert result1 is result2

    @patch("wrds_mcp.tools.catalog.get_wrds_connection")
    @patch("wrds_mcp.tools.catalog._check_schema_exists", return_value=True)
    @patch("wrds_mcp.tools.catalog._query_date_range", return_value={
        "earliest": "2000-01-01", "latest": "2024-12-31", "row_count": 1000000,
    })
    def test_refresh_bypasses_cache(self, mock_dates, mock_schema, mock_conn):
        result1 = get_data_catalog()
        result2 = get_data_catalog(refresh=True)

        assert mock_conn.call_count == 2

    @patch("wrds_mcp.tools.catalog.get_wrds_connection")
    @patch("wrds_mcp.tools.catalog._check_schema_exists", return_value=False)
    def test_unavailable_schemas(self, mock_schema, mock_conn):
        result = get_data_catalog()

        assert result["equity"] == {"available": False}
        assert result["bonds"] == {"available": False}


class TestGetTableSchema:
    @patch("wrds_mcp.tools.catalog.get_wrds_connection")
    def test_happy_path(self, mock_get_conn):
        col_df = pd.DataFrame({
            "column_name": ["gvkey", "datadate", "sale"],
            "data_type": ["character varying", "date", "double precision"],
            "is_nullable": ["NO", "NO", "YES"],
        })
        row_count_df = pd.DataFrame({"approx_rows": [5000000]})

        conn = MagicMock()
        conn.raw_sql.side_effect = [col_df, row_count_df]
        mock_get_conn.return_value = conn

        result = get_table_schema("comp", "funda")

        assert result["schema"] == "comp"
        assert result["table"] == "funda"
        assert len(result["columns"]) == 3
        assert result["columns"][0]["name"] == "gvkey"
        assert result["row_count"] == 5000000
        # Should have description from COLUMN_DOCS
        assert result["columns"][0]["description"] is not None

    def test_disallowed_schema(self):
        with pytest.raises(ToolError, match="not in the allowlist"):
            get_table_schema("pg_catalog", "pg_tables")

    @patch("wrds_mcp.tools.catalog.get_wrds_connection")
    def test_table_not_found(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame()
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="not found"):
            get_table_schema("comp", "nonexistent_table")

    @patch("wrds_mcp.tools.catalog.get_wrds_connection")
    def test_case_insensitive(self, mock_get_conn):
        col_df = pd.DataFrame({
            "column_name": ["gvkey"],
            "data_type": ["character varying"],
            "is_nullable": ["NO"],
        })
        row_count_df = pd.DataFrame({"approx_rows": [100]})

        conn = MagicMock()
        conn.raw_sql.side_effect = [col_df, row_count_df]
        mock_get_conn.return_value = conn

        result = get_table_schema("COMP", "FUNDA")

        assert result["schema"] == "comp"
        assert result["table"] == "funda"

    @patch("wrds_mcp.tools.catalog.get_wrds_connection")
    def test_row_count_failure_graceful(self, mock_get_conn):
        col_df = pd.DataFrame({
            "column_name": ["gvkey"],
            "data_type": ["character varying"],
            "is_nullable": ["NO"],
        })

        conn = MagicMock()
        conn.raw_sql.side_effect = [col_df, Exception("permission denied")]
        mock_get_conn.return_value = conn

        result = get_table_schema("comp", "funda")

        assert result["row_count"] is None
        assert len(result["columns"]) == 1


class TestResolveIdentifier:
    @patch("wrds_mcp.tools.catalog.get_wrds_connection")
    @patch("wrds_mcp.tools.catalog.resolve_ticker_to_gvkey", return_value="001234")
    def test_resolve_gvkey(self, mock_resolve, mock_conn):
        result = resolve_identifier("AAPL", target="gvkey")

        assert result["ticker"] == "AAPL"
        assert result["target_type"] == "gvkey"
        assert result["value"] == "001234"

    @patch("wrds_mcp.tools.catalog.get_wrds_connection")
    def test_resolve_permno(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame({"permno": [14593]})
        mock_get_conn.return_value = conn

        result = resolve_identifier("AAPL", target="permno")

        assert result["value"] == 14593
        assert result["target_type"] == "permno"

    @patch("wrds_mcp.tools.catalog.get_wrds_connection")
    @patch("wrds_mcp.tools.catalog.resolve_ticker_to_fisd_issuer", return_value=98765)
    def test_resolve_issuer_id(self, mock_resolve, mock_conn):
        result = resolve_identifier("AAPL", target="issuer_id")

        assert result["value"] == 98765
        assert result["target_type"] == "issuer_id"

    def test_invalid_target(self):
        with pytest.raises(ToolError, match="Invalid target"):
            resolve_identifier("AAPL", target="cusip")

    @patch("wrds_mcp.tools.catalog.get_wrds_connection")
    @patch("wrds_mcp.tools.catalog.resolve_ticker_to_gvkey", return_value=None)
    def test_not_found(self, mock_resolve, mock_conn):
        with pytest.raises(ToolError, match="Could not resolve"):
            resolve_identifier("ZZZZ", target="gvkey")

    def test_empty_ticker(self):
        with pytest.raises(ToolError, match="cannot be empty"):
            resolve_identifier("", target="gvkey")

    def test_invalid_ticker(self):
        with pytest.raises(ToolError, match="Invalid ticker"):
            resolve_identifier("AA@#", target="gvkey")

    @patch("wrds_mcp.tools.catalog.get_wrds_connection")
    def test_permno_not_found(self, mock_get_conn):
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame()
        mock_get_conn.return_value = conn

        with pytest.raises(ToolError, match="Could not resolve"):
            resolve_identifier("ZZZZ", target="permno")
