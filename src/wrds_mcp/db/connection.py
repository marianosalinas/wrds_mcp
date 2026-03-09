"""WRDS connection management with singleton pattern and retry logic."""

import logging
import os
import time
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, text
from fastmcp.server.lifespan import lifespan

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
BACKOFF_BASE = 1  # seconds

WRDS_HOST = "wrds-pgdata.wharton.upenn.edu"
WRDS_PORT = 9737
WRDS_DB = "wrds"


class WRDSConnection:
    """Thin wrapper around a SQLAlchemy engine to provide a raw_sql interface
    compatible with the wrds library API."""

    def __init__(self, engine):
        self._engine = engine

    def raw_sql(self, sql, params=None, date_cols=None, **kwargs):
        """Execute a SQL query and return a DataFrame."""
        with self._engine.connect() as conn:
            result = pd.read_sql_query(
                text(sql) if isinstance(sql, str) else sql,
                con=conn,
                params=params,
                parse_dates=date_cols,
            )
        return result

    def close(self):
        """Dispose of the engine."""
        self._engine.dispose()


class WRDSConnectionManager:
    """Singleton WRDS connection manager with retry logic.

    Reads credentials from WRDS_USERNAME and WRDS_PASSWORD environment
    variables. Never accepts hardcoded values.
    """

    _instance = None
    _connection = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def connect(self) -> WRDSConnection:
        """Establish a WRDS connection with retry logic.

        Retries up to 3 times with exponential backoff (1s, 2s, 4s).

        Returns:
            WRDSConnection: An authenticated WRDS connection.

        Raises:
            ConnectionError: If all retry attempts fail.
            ValueError: If credentials are not set in environment.
        """
        username = os.environ.get("WRDS_USERNAME")
        password = os.environ.get("WRDS_PASSWORD")

        if not username or not password:
            raise ValueError(
                "WRDS_USERNAME and WRDS_PASSWORD must be set as environment variables. "
                "Copy .env.example to .env and fill in your credentials."
            )

        if self._connection is not None:
            try:
                # Test if connection is still alive
                self._connection.raw_sql("SELECT 1")
                return self._connection
            except Exception:
                logger.debug("Existing WRDS connection is stale, reconnecting.")
                self._connection = None

        last_error = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.debug("WRDS connection attempt %d/%d", attempt, MAX_RETRIES)
                encoded_pwd = quote_plus(password)
                engine = create_engine(
                    f"postgresql+psycopg2://{username}:{encoded_pwd}"
                    f"@{WRDS_HOST}:{WRDS_PORT}/{WRDS_DB}?sslmode=require",
                    pool_pre_ping=True,
                )
                conn = WRDSConnection(engine)
                # Verify connection works
                conn.raw_sql("SELECT 1")
                self._connection = conn
                logger.info("WRDS connection established.")
                return conn
            except Exception as e:
                last_error = e
                if attempt < MAX_RETRIES:
                    wait = BACKOFF_BASE * (2 ** (attempt - 1))
                    logger.warning(
                        "WRDS connection attempt %d failed: %s. Retrying in %ds.",
                        attempt, e, wait,
                    )
                    time.sleep(wait)

        raise ConnectionError(
            f"Failed to connect to WRDS after {MAX_RETRIES} attempts: {last_error}"
        )

    def close(self):
        """Close the WRDS connection."""
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:
                pass
            self._connection = None
            logger.info("WRDS connection closed.")

    @classmethod
    def reset(cls):
        """Reset the singleton (for testing)."""
        if cls._instance is not None:
            cls._instance.close()
        cls._instance = None
        cls._connection = None


def get_wrds_connection() -> WRDSConnection:
    """Get the singleton WRDS connection."""
    return WRDSConnectionManager().connect()


def resolve_ticker_to_gvkey(conn: WRDSConnection, ticker: str) -> str | None:
    """Resolve a ticker symbol to a Compustat gvkey.

    Args:
        conn: Active WRDS connection.
        ticker: Company ticker symbol (e.g., 'AAPL').

    Returns:
        The gvkey string, or None if not found.
    """
    logger.debug("Resolving ticker '%s' to gvkey", ticker)
    df = conn.raw_sql(
        """
        SELECT DISTINCT gvkey
        FROM comp.security
        WHERE tic = :ticker
        ORDER BY gvkey
        LIMIT 1
        """,
        params={"ticker": ticker.upper()},
    )
    if df.empty:
        return None
    return str(df.iloc[0]["gvkey"])


@lifespan
async def wrds_lifespan(server):
    """FastMCP lifespan: connect to WRDS on startup, close on shutdown."""
    manager = WRDSConnectionManager()
    try:
        conn = manager.connect()
        yield {"wrds_conn": conn, "wrds_manager": manager}
    finally:
        manager.close()
