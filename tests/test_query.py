"""Tests for guarded SQL query tool."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastmcp.exceptions import ToolError

from wrds_mcp.tools.query import (
    _strip_comments,
    _strip_string_literals,
    _validate_query,
    query_wrds,
    MAX_LIMIT,
)


class TestStripComments:
    def test_line_comment(self):
        assert "SELECT 1" in _strip_comments("SELECT 1 -- comment")

    def test_block_comment(self):
        result = _strip_comments("SELECT /* stuff */ 1")
        assert "SELECT" in result and "1" in result
        assert "stuff" not in result

    def test_no_comments(self):
        assert _strip_comments("SELECT 1") == "SELECT 1"


class TestStripStringLiterals:
    def test_single_quoted(self):
        result = _strip_string_literals("WHERE x = 'DELETE FROM foo'")
        assert "DELETE" not in result
        assert "'_STR_'" in result

    def test_escaped_quotes(self):
        result = _strip_string_literals("WHERE x = 'it''s fine'")
        assert "'_STR_'" in result


class TestValidateQuery:
    def test_valid_select(self):
        sql, warnings = _validate_query("SELECT * FROM comp.funda LIMIT 10")
        assert "SELECT" in sql
        assert "LIMIT 10" in sql

    def test_valid_with_cte(self):
        sql, _ = _validate_query("WITH cte AS (SELECT 1) SELECT * FROM cte")
        assert sql.startswith("WITH")

    def test_rejects_insert(self):
        with pytest.raises(ToolError, match="Only SELECT"):
            _validate_query("INSERT INTO comp.funda VALUES (1)")

    def test_rejects_delete_keyword(self):
        with pytest.raises(ToolError, match="disallowed keyword"):
            _validate_query("SELECT * FROM comp.funda; DELETE FROM comp.funda")

    def test_rejects_drop_keyword(self):
        with pytest.raises(ToolError, match="disallowed keyword"):
            _validate_query("SELECT * FROM comp.funda; DROP TABLE comp.funda")

    def test_allows_delete_in_string_literal(self):
        sql, _ = _validate_query("SELECT * FROM comp.funda WHERE x = 'DELETE'")
        assert sql is not None

    def test_rejects_disallowed_schema(self):
        with pytest.raises(ToolError, match="not in the allowlist"):
            _validate_query("SELECT * FROM public.sometable")

    def test_allows_valid_schemas(self):
        for schema in ["comp", "crsp", "trace", "wrdsapps_bondret", "fisd", "dealscan"]:
            sql, _ = _validate_query(f"SELECT 1 FROM {schema}.some_table")
            assert sql is not None

    def test_appends_limit_if_missing(self):
        sql, _ = _validate_query("SELECT * FROM comp.funda")
        assert f"LIMIT {MAX_LIMIT}" in sql

    def test_reduces_excessive_limit(self):
        sql, warnings = _validate_query("SELECT * FROM comp.funda LIMIT 99999")
        assert f"LIMIT {MAX_LIMIT}" in sql
        assert any("exceeds maximum" in w for w in warnings)

    def test_preserves_valid_limit(self):
        sql, _ = _validate_query("SELECT * FROM comp.funda LIMIT 50")
        assert "LIMIT 50" in sql

    def test_empty_query(self):
        with pytest.raises(ToolError, match="Empty query"):
            _validate_query("-- just a comment")

    def test_funda_without_indfmt_warning(self):
        _, warnings = _validate_query("SELECT * FROM comp.funda WHERE gvkey = '001234'")
        assert any("indfmt" in w for w in warnings)

    def test_funda_with_indfmt_no_warning(self):
        _, warnings = _validate_query(
            "SELECT * FROM comp.funda WHERE gvkey = '001234' AND indfmt = 'INDL'"
        )
        assert not any("indfmt" in w for w in warnings)

    def test_strips_trailing_semicolon(self):
        sql, _ = _validate_query("SELECT * FROM comp.funda;")
        assert not sql.rstrip().endswith(";")


class TestQueryWrds:
    @patch("wrds_mcp.tools.query.get_wrds_connection")
    def test_happy_path(self, mock_get_conn):
        mock_engine = MagicMock()
        mock_db_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_db_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        conn = MagicMock()
        conn._engine = mock_engine
        mock_get_conn.return_value = conn

        df = pd.DataFrame({"gvkey": ["001234"], "sale": [100.0]})

        with patch("wrds_mcp.tools.query.pd.read_sql_query", return_value=df):
            result = query_wrds("SELECT gvkey, sale FROM comp.funda LIMIT 1")

        assert result["row_count"] == 1
        assert result["rows"][0]["gvkey"] == "001234"
        assert result["source"] == "query_wrds"

    def test_rejects_mutation(self):
        with pytest.raises(ToolError, match="Only SELECT"):
            query_wrds("DROP TABLE comp.funda")

    @patch("wrds_mcp.tools.query.get_wrds_connection")
    def test_timeout_error(self, mock_get_conn):
        mock_engine = MagicMock()
        mock_db_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_db_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        conn = MagicMock()
        conn._engine = mock_engine
        mock_get_conn.return_value = conn

        with patch(
            "wrds_mcp.tools.query.pd.read_sql_query",
            side_effect=Exception("statement timeout"),
        ):
            with pytest.raises(ToolError, match="timed out"):
                query_wrds("SELECT * FROM comp.funda")

    @patch("wrds_mcp.tools.query.get_wrds_connection")
    def test_generic_error(self, mock_get_conn):
        mock_engine = MagicMock()
        mock_db_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_db_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        conn = MagicMock()
        conn._engine = mock_engine
        mock_get_conn.return_value = conn

        with patch(
            "wrds_mcp.tools.query.pd.read_sql_query",
            side_effect=Exception("connection reset"),
        ):
            with pytest.raises(ToolError, match="query execution failed"):
                query_wrds("SELECT * FROM comp.funda")

    @patch("wrds_mcp.tools.query.get_wrds_connection")
    def test_with_params(self, mock_get_conn):
        mock_engine = MagicMock()
        mock_db_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_db_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        conn = MagicMock()
        conn._engine = mock_engine
        mock_get_conn.return_value = conn

        df = pd.DataFrame({"gvkey": ["001234"]})

        with patch("wrds_mcp.tools.query.pd.read_sql_query", return_value=df):
            result = query_wrds(
                "SELECT gvkey FROM comp.funda WHERE tic = :ticker LIMIT 1",
                params={"ticker": "AAPL"},
            )

        assert result["row_count"] == 1

    @patch("wrds_mcp.tools.query.get_wrds_connection")
    def test_warnings_passed_through(self, mock_get_conn):
        mock_engine = MagicMock()
        mock_db_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_db_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        conn = MagicMock()
        conn._engine = mock_engine
        mock_get_conn.return_value = conn

        df = pd.DataFrame({"gvkey": ["001234"]})

        with patch("wrds_mcp.tools.query.pd.read_sql_query", return_value=df):
            result = query_wrds("SELECT gvkey FROM comp.funda WHERE gvkey = '001234'")

        # Should have indfmt warning
        assert len(result["warnings"]) > 0
