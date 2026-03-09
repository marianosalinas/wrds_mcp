"""Shared test fixtures for wrds-mcp tests."""

from unittest.mock import MagicMock

import pandas as pd
import pytest


@pytest.fixture
def mock_wrds_conn():
    """Mock WRDS connection that never hits the real API."""
    conn = MagicMock()
    conn.raw_sql = MagicMock(return_value=pd.DataFrame())
    conn.close = MagicMock()
    return conn


@pytest.fixture
def sample_funda_df():
    """Realistic Compustat annual fundamentals DataFrame."""
    return pd.DataFrame({
        "gvkey": ["001690"] * 3,
        "datadate": pd.to_datetime(["2022-09-30", "2023-09-30", "2024-09-30"]),
        "fyear": [2022, 2023, 2024],
        "at": [352583.0, 352755.0, 364980.0],
        "lt": [302083.0, 290437.0, 308030.0],
        "dltt": [98959.0, 95281.0, 96813.0],
        "dlc": [21110.0, 18387.0, 17382.0],
        "seq": [50672.0, 62158.0, 56950.0],
        "che": [23646.0, 29965.0, 29943.0],
        "oibdp": [130541.0, 123380.0, 132916.0],
        "xint": [2931.0, 3933.0, 3755.0],
        "xrent": [None, None, None],
        "act": [135405.0, 143566.0, 152987.0],
        "lct": [153982.0, 145308.0, 176392.0],
        "ivst": [20729.0, 31590.0, 35228.0],
        "sale": [394328.0, 383285.0, 391035.0],
    })


@pytest.fixture
def sample_trace_df():
    """Realistic TRACE enhanced DataFrame."""
    return pd.DataFrame({
        "cusip_id": ["037833AK6"] * 5,
        "trd_exctn_dt": pd.to_datetime([
            "2024-01-15", "2024-01-15", "2024-01-16", "2024-01-16", "2024-01-17",
        ]),
        "trd_exctn_tm": ["10:30:00", "14:15:00", "09:45:00", "11:20:00", "15:00:00"],
        "rptd_pr": [99.5, 99.6, 99.4, 99.7, 99.8],
        "yld_pt": [4.52, 4.51, 4.53, 4.50, 4.49],
        "entrd_vol_qt": [1000000, 500000, 2000000, 750000, 1500000],
        "rpt_side_cd": ["B", "S", "B", "B", "S"],
        "bond_sym_id": ["AAPL.GX"] * 5,
    })


@pytest.fixture
def sample_ratings_df():
    """Realistic Compustat S&P ratings DataFrame."""
    return pd.DataFrame({
        "gvkey": ["001690"] * 4,
        "datadate": pd.to_datetime([
            "2014-01-15", "2015-03-20", "2016-06-10", "2017-01-05",
        ]),
        "splticrm": ["AA+", "AA+", "AA+", "AA+"],
        "spsdrm": [None, None, None, None],
        "spsticrm": ["A-1+", "A-1+", "A-1+", "A-1+"],
    })


@pytest.fixture
def sample_fisd_df():
    """Realistic FISD merged issue DataFrame."""
    return pd.DataFrame({
        "complete_cusip": ["037833AK6", "037833AL4", "037833AM2"],
        "issue_id": [1001, 1002, 1003],
        "issuer_id": [500, 500, 500],
        "maturity": pd.to_datetime(["2026-05-01", "2029-02-15", "2031-08-01"]),
        "offering_amt": [2000000, 1500000, 3000000],
        "offering_date": pd.to_datetime(["2016-05-01", "2019-02-15", "2021-08-01"]),
        "coupon": [2.25, 3.05, 1.70],
        "coupon_type": ["F", "F", "F"],
        "security_level": ["SEN", "SEN", "SEN"],
        "bond_type": ["CDEB", "CDEB", "CMTN"],
        "asset_backed": ["N", "N", "N"],
        "convertible": ["N", "N", "N"],
    })


@pytest.fixture
def sample_gvkey_df():
    """DataFrame for ticker-to-gvkey resolution."""
    return pd.DataFrame({"gvkey": ["001690"]})
