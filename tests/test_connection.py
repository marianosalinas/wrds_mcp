"""Tests for WRDS connection management."""

import os
from unittest.mock import MagicMock, patch

import pytest

from wrds_mcp.db.connection import (
    WRDSConnectionManager,
    get_wrds_connection,
    resolve_ticker_to_gvkey,
)
import pandas as pd


@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset the singleton before each test."""
    WRDSConnectionManager._instance = None
    WRDSConnectionManager._connection = None
    yield
    WRDSConnectionManager._instance = None
    WRDSConnectionManager._connection = None


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

    @patch("wrds_mcp.db.connection.wrds.Connection")
    def test_successful_connection(self, mock_wrds_conn):
        mock_conn = MagicMock()
        mock_wrds_conn.return_value = mock_conn

        with patch.dict(os.environ, {"WRDS_USERNAME": "user", "WRDS_PASSWORD": "pass"}):
            manager = WRDSConnectionManager()
            result = manager.connect()

        assert result is mock_conn
        mock_wrds_conn.assert_called_once_with(
            wrds_username="user", wrds_password="pass"
        )

    @patch("wrds_mcp.db.connection.wrds.Connection")
    def test_reuses_existing_connection(self, mock_wrds_conn):
        mock_conn = MagicMock()
        mock_conn.raw_sql.return_value = None  # health check passes
        mock_wrds_conn.return_value = mock_conn

        with patch.dict(os.environ, {"WRDS_USERNAME": "user", "WRDS_PASSWORD": "pass"}):
            manager = WRDSConnectionManager()
            conn1 = manager.connect()
            conn2 = manager.connect()

        assert conn1 is conn2
        assert mock_wrds_conn.call_count == 1  # only created once

    @patch("wrds_mcp.db.connection.wrds.Connection")
    def test_reconnects_on_stale_connection(self, mock_wrds_conn):
        stale_conn = MagicMock()
        stale_conn.raw_sql.side_effect = Exception("Connection closed")
        fresh_conn = MagicMock()
        mock_wrds_conn.side_effect = [stale_conn, fresh_conn]

        with patch.dict(os.environ, {"WRDS_USERNAME": "user", "WRDS_PASSWORD": "pass"}):
            manager = WRDSConnectionManager()
            conn1 = manager.connect()
            assert conn1 is stale_conn

            # Now the health check fails, should reconnect
            conn2 = manager.connect()
            assert conn2 is fresh_conn

    @patch("wrds_mcp.db.connection.time.sleep")
    @patch("wrds_mcp.db.connection.wrds.Connection")
    def test_retry_on_failure(self, mock_wrds_conn, mock_sleep):
        mock_conn = MagicMock()
        mock_wrds_conn.side_effect = [Exception("Fail 1"), Exception("Fail 2"), mock_conn]

        with patch.dict(os.environ, {"WRDS_USERNAME": "user", "WRDS_PASSWORD": "pass"}):
            manager = WRDSConnectionManager()
            result = manager.connect()

        assert result is mock_conn
        assert mock_wrds_conn.call_count == 3
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(1)
        mock_sleep.assert_any_call(2)

    @patch("wrds_mcp.db.connection.time.sleep")
    @patch("wrds_mcp.db.connection.wrds.Connection")
    def test_all_retries_fail(self, mock_wrds_conn, mock_sleep):
        mock_wrds_conn.side_effect = Exception("Always fails")

        with patch.dict(os.environ, {"WRDS_USERNAME": "user", "WRDS_PASSWORD": "pass"}):
            manager = WRDSConnectionManager()
            with pytest.raises(ConnectionError, match="Failed to connect"):
                manager.connect()

        assert mock_wrds_conn.call_count == 3

    @patch("wrds_mcp.db.connection.wrds.Connection")
    def test_close(self, mock_wrds_conn):
        mock_conn = MagicMock()
        mock_wrds_conn.return_value = mock_conn

        with patch.dict(os.environ, {"WRDS_USERNAME": "user", "WRDS_PASSWORD": "pass"}):
            manager = WRDSConnectionManager()
            manager.connect()
            manager.close()

        mock_conn.close.assert_called_once()

    def test_close_when_no_connection(self):
        manager = WRDSConnectionManager()
        manager.close()  # should not raise

    @patch("wrds_mcp.db.connection.wrds.Connection")
    def test_close_handles_exception(self, mock_wrds_conn):
        mock_conn = MagicMock()
        mock_conn.close.side_effect = Exception("Close error")
        mock_wrds_conn.return_value = mock_conn

        with patch.dict(os.environ, {"WRDS_USERNAME": "user", "WRDS_PASSWORD": "pass"}):
            manager = WRDSConnectionManager()
            manager.connect()
            manager.close()  # should not raise

    def test_reset(self):
        manager = WRDSConnectionManager()
        WRDSConnectionManager.reset()
        assert WRDSConnectionManager._instance is None
        assert WRDSConnectionManager._connection is None


class TestGetWrdsConnection:
    """Tests for get_wrds_connection helper."""

    @patch("wrds_mcp.db.connection.wrds.Connection")
    def test_returns_connection(self, mock_wrds_conn):
        mock_conn = MagicMock()
        mock_wrds_conn.return_value = mock_conn

        with patch.dict(os.environ, {"WRDS_USERNAME": "user", "WRDS_PASSWORD": "pass"}):
            result = get_wrds_connection()

        assert result is mock_conn


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
