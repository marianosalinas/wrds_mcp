# wrds-mcp

MCP server providing Claude Code with natural language access to WRDS financial data for credit analysis.

## Quick Reference

- **Run tests:** `python -m pytest tests/ -v`
- **Run server:** `wrds-mcp` or `fastmcp run src/wrds_mcp/server.py:mcp`
- **Install dev:** `pip install -e ".[dev]"`

## Architecture

```
src/wrds_mcp/
├── server.py           # FastMCP entry point, mounts sub-servers
├── db/connection.py    # Singleton WRDS connection with retry (3 attempts, exponential backoff)
└── tools/
    ├── _validation.py  # Input validation + DataFrame-to-JSON conversion
    ├── bonds.py        # TRACE/FISD bond tools (3 tools)
    ├── ratings.py      # S&P ratings from comp.adsprate (2 tools)
    └── financials.py   # Compustat leverage/coverage/liquidity (4 tools)
```

**Pattern:** Each tool module creates its own `FastMCP` sub-server, mounted by `server.py`. Tools use `get_wrds_connection()` singleton — easy to mock in tests.

## WRDS Tables Used

| Table | Purpose |
|-------|---------|
| `trace.trace_enhanced` | Bond transaction data (price, yield, volume) |
| `fisd.fisd_mergedissue` | Bond characteristics (CUSIP, coupon, maturity) |
| `fisd.fisd_mergedissuer` | Issuer info (ticker linkage) |
| `comp.funda` | Annual financial fundamentals |
| `comp.adsprate` | S&P credit ratings (through Feb 2017 only) |
| `comp.security` | Ticker-to-gvkey resolution |

## Conventions

- **Commits:** conventional commits (`feat:`, `fix:`, `test:`, `docs:`)
- **All tools:** validate inputs, catch query errors as `ToolError`, return JSON-serializable dicts (never raw DataFrames), log queries at DEBUG
- **NaN handling:** NaN/NaT → `None`, Timestamps → ISO date strings, Inf → `None`
- **Empty results:** return `[{"message": "..."}]` not an error
- **Compustat filter:** always apply `indfmt='INDL' AND datafmt='STD' AND consol='C' AND curcd='USD'`
- **Credentials:** env vars only (`WRDS_USERNAME`, `WRDS_PASSWORD`), never hardcoded
- **Tests:** mock `get_wrds_connection` and `resolve_ticker_to_gvkey`, never hit real WRDS API

## Tool Inventory (10 tools)

### bonds.py
- `get_bond_transactions(ticker, start_date, end_date)` — TRACE transactions
- `get_bond_yield_history(cusip, start_date, end_date)` — daily VWAP yield series
- `get_company_bonds(ticker)` — outstanding bonds from FISD

### ratings.py
- `get_credit_ratings(ticker)` — latest S&P rating
- `get_ratings_history(ticker, start_date, end_date)` — rating changes with direction

### financials.py
- `get_leverage_metrics(ticker, periods=5)` — debt/EBITDA, net debt/EBITDA
- `get_coverage_ratios(ticker, periods=5)` — interest coverage, FCC
- `get_liquidity_metrics(ticker, periods=5)` — current ratio, cash
- `get_credit_summary(ticker)` — combined snapshot (calls other tools)
