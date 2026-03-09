"""Tests for WRDS connection management."""

import os
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from wrds_mcp.db.connection import (
    WRDSConnectionManager,
    WRDSConnection,
    get_wrds_connection,
    resolve_ticker_to_gvkey,
)


@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset the singleton before each test."""
    WRDSConnectionManager._instance = None
    WRDSConnectionManager._connection = None
    yield
    WRDSConnectionManager._instance = None
    WRDSConnectionManager._connection = None


def _mock_engine():
    """Create a mock SQLAlchemy engine with working context manager."""
    engine = MagicMock()
    conn_cm = MagicMock()
    engine.connect.return_value = conn_cm
    conn_cm.__enter__ = MagicMock(return_value=MagicMock())
    conn_cm.__exit__ = MagicMock(return_value=False)
    return engine


class TestWRDSConnection:
    """Tests for WRDSConnection wrapper."""

    @patch("wrds_mcp.db.connection.pd.read_sql_query", return_value=pd.DataFrame({"test": [1]}))
    def test_raw_sql(self, mock_read):
        conn = WRDSConnection(_mock_engine())
        result = conn.raw_sql("SELECT 1")
        assert not result.empty

    def test_close(self):
        engine = _mock_engine()
        conn = WRDSConnection(engine)
        conn.close()
        engine.dispose.assert_called_once()


class TestWRDSConnectionManager:
    """Tests for WRDSConnectionManager."""

    def test_singleton(self):
        a = WRDSConnectionManager()
        b = WRDSConnectionManager()
        assert a is b

    def test_missing_credentials(self):
        with patch.dict(os.environ, {}, clear=True):
            manager = WRDSConnectionManager()
            with pytest.raises(ValueError, match="WRDS_USERNAME"):
                manager.connect()

    def test_missing_password(self):
        with patch.dict(os.environ, {"WRDS_USERNAME": "user"}, clear=True):
            manager = WRDSConnectionManager()
            with pytest.raises(ValueError, match="WRDS_PASSWORD"):
                manager.connect()

    @patch("wrds_mcp.db.connection.pd.read_sql_query", return_value=pd.DataFrame({"test": [1]}))
    @patch("wrds_mcp.db.connection.create_engine")
    def test_successful_connection(self, mock_create_engine, mock_read):
        mock_create_engine.return_value = _mock_engine()

        with patch.dict(os.environ, {"WRDS_USERNAME": "user", "WRDS_PASSWORD": "pass"}):
            manager = WRDSConnectionManager()
            result = manager.connect()

        assert isinstance(result, WRDSConnection)
        mock_create_engine.assert_called_once()

    @patch("wrds_mcp.db.connection.pd.read_sql_query", return_value=pd.DataFrame({"test": [1]}))
    @patch("wrds_mcp.db.connection.create_engine")
    def test_reuses_existing_connection(self, mock_create_engine, mock_read):
        mock_create_engine.return_value = _mock_engine()

        with patch.dict(os.environ, {"WRDS_USERNAME": "user", "WRDS_PASSWORD": "pass"}):
            manager = WRDSConnectionManager()
            conn1 = manager.connect()
            conn2 = manager.connect()

        assert conn1 is conn2
        assert mock_create_engine.call_count == 1

    @patch("wrds_mcp.db.connection.pd.read_sql_query", return_value=pd.DataFrame({"test": [1]}))
    @patch("wrds_mcp.db.connection.time.sleep")
    @patch("wrds_mcp.db.connection.create_engine")
    def test_retry_on_failure(self, mock_create_engine, mock_sleep, mock_read):
        mock_create_engine.side_effect = [
            Exception("Fail 1"),
            Exception("Fail 2"),
            _mock_engine(),
        ]

        with patch.dict(os.environ, {"WRDS_USERNAME": "user", "WRDS_PASSWORD": "pass"}):
            manager = WRDSConnectionManager()
            result = manager.connect()

        assert isinstance(result, WRDSConnection)
        assert mock_create_engine.call_count == 3
        assert mock_sleep.call_count == 2

    @patch("wrds_mcp.db.connection.time.sleep")
    @patch("wrds_mcp.db.connection.create_engine")
    def test_all_retries_fail(self, mock_create_engine, mock_sleep):
        mock_create_engine.side_effect = Exception("Always fails")

        with patch.dict(os.environ, {"WRDS_USERNAME": "user", "WRDS_PASSWORD": "pass"}):
            manager = WRDSConnectionManager()
            with pytest.raises(ConnectionError, match="Failed to connect"):
                manager.connect()

        assert mock_create_engine.call_count == 3

    @patch("wrds_mcp.db.connection.pd.read_sql_query", return_value=pd.DataFrame({"test": [1]}))
    @patch("wrds_mcp.db.connection.create_engine")
    def test_close(self, mock_create_engine, mock_read):
        engine = _mock_engine()
        mock_create_engine.return_value = engine

        with patch.dict(os.environ, {"WRDS_USERNAME": "user", "WRDS_PASSWORD": "pass"}):
            manager = WRDSConnectionManager()
            manager.connect()
            manager.close()

        engine.dispose.assert_called_once()

    def test_close_when_no_connection(self):
        manager = WRDSConnectionManager()
        manager.close()  # should not raise

    def test_reset(self):
        manager = WRDSConnectionManager()
        WRDSConnectionManager.reset()
        assert WRDSConnectionManager._instance is None
        assert WRDSConnectionManager._connection is None


class TestGetWrdsConnection:
    """Tests for get_wrds_connection helper."""

    @patch("wrds_mcp.db.connection.pd.read_sql_query", return_value=pd.DataFrame({"test": [1]}))
    @patch("wrds_mcp.db.connection.create_engine")
    def test_returns_connection(self, mock_create_engine, mock_read):
        mock_create_engine.return_value = _mock_engine()

        with patch.dict(os.environ, {"WRDS_USERNAME": "user", "WRDS_PASSWORD": "pass"}):
            result = get_wrds_connection()

        assert isinstance(result, WRDSConnection)


class TestResolveTickerToGvkey:
    """Tests for resolve_ticker_to_gvkey."""

    def test_found(self):
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame({"gvkey": ["001690"]})

        result = resolve_ticker_to_gvkey(conn, "AAPL")
        assert result == "001690"

    def test_not_found(self):
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame()

        result = resolve_ticker_to_gvkey(conn, "ZZZZ")
        assert result is None

    def test_normalizes_ticker(self):
        conn = MagicMock()
        conn.raw_sql.return_value = pd.DataFrame({"gvkey": ["001690"]})

        resolve_ticker_to_gvkey(conn, "aapl")

        call_args = conn.raw_sql.call_args
        assert call_args[1]["params"]["ticker"] == "AAPL"
