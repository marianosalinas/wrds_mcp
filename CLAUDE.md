# wrds-mcp

MCP server providing Claude Code with natural language access to WRDS financial data for credit and equity analysis.

## Quick Reference

- **Run tests:** `python -m pytest tests/ -v`
- **Run server:** `wrds-mcp` or `fastmcp run src/wrds_mcp/server.py:mcp`
- **Install dev:** `pip install -e ".[dev]"`

## Architecture

```
src/wrds_mcp/
├── server.py           # FastMCP entry point, mounts 6 sub-servers
├── db/connection.py    # Singleton WRDS connection with retry (3 attempts, exponential backoff)
└── tools/
    ├── _validation.py  # Input validation + DataFrame-to-JSON conversion
    ├── catalog.py      # Discovery tool — live data catalog with date ranges (1 tool)
    ├── equity.py       # CRSP stock tools — price history, returns, summary (3 tools)
    ├── bonds.py        # TRACE/FISD/bondret — transactions, prices, returns, covenants (6 tools)
    ├── ratings.py      # Credit ratings from bondret + Compustat fallback (2 tools)
    ├── financials.py   # Compustat leverage/coverage/liquidity + composites (5 tools)
    └── loans.py        # DealScan syndicated loan terms + covenants (2 tools)
```

**Pattern:** Each tool module creates its own `FastMCP` sub-server, mounted by `server.py`. Tools use `get_wrds_connection()` singleton — easy to mock in tests.

## WRDS Tables Used

| Table | Purpose |
|-------|---------|
| `crsp.dsf_v2` | Daily stock prices, returns, volume, market cap |
| `crsp.msf_v2` | Monthly stock data (auto-selected for long ranges) |
| `trace.trace` | Raw FINRA TRACE bond transactions (most current, needs filtering) |
| `trace.trace_btds144a` | 144A private placement bond transactions (auto-fallback) |
| `wrdsapps_bondret.trace_enhanced_clean` | Cleaned TRACE (research quality, ~12 month lag) |
| `wrdsapps_bondret.bondret` | Monthly bond returns, yield, spread, duration, multi-agency ratings |
| `fisd.fisd_mergedissue` | Bond characteristics (CUSIP, coupon, maturity) |
| `fisd.fisd_mergedissuer` | Issuer info (ticker linkage) |
| `fisd.fisd_bondholder_protective` | Bond covenants (cross-default, negative pledge, etc.) |
| `fisd.fisd_call_schedule` | Call provisions |
| `fisd.fisd_put_schedule` | Put provisions |
| `fisd.fisd_sinking_fund` | Sinking fund provisions |
| `comp.funda` | Annual financial fundamentals |
| `comp.adsprate` | S&P credit ratings (through Feb 2017 only — fallback) |
| `comp.security` | Ticker-to-gvkey resolution |
| `dealscan.facility` | Syndicated loan facility details |
| `dealscan.package` | Loan deal packages |
| `dealscan.borrower` | Borrower linkage |
| `dealscan.company` | Company ticker linkage |
| `dealscan.currfacpricing` | Loan pricing (spreads) |
| `dealscan.financialcovenant` | Financial covenants on loans |
| `dealscan.networthcovenant` | Net worth covenants on loans |

## Key Design Decisions

- **Auto-routing:** Bond price/yield tools auto-route between `trace_enhanced_clean` (historical, research quality) and `trace.trace` (recent, raw) based on whether end_date is within the last ~12 months
- **144A fallback:** When standard TRACE tables return empty, bond tools automatically check `trace.trace_btds144a` for 144A private placements (e.g., PRKS/SeaWorld bonds)
- **Discovery tool:** `get_data_catalog()` queries live date ranges so Claude knows what data exists and which tool to use
- **Ratings primary source:** `wrdsapps_bondret.bondret` provides S&P + Moody's + Fitch through latest month; `comp.adsprate` is fallback only (ended Feb 2017)
- **FISD ticker matching:** Uses issuer_id subquery pattern because many bonds have NULL ticker in fisd_mergedissue. Falls back to Compustat CUSIP linkage (5-char prefix match) when FISD has no ticker at all

## Conventions

- **Commits:** conventional commits (`feat:`, `fix:`, `test:`, `docs:`)
- **All tools:** validate inputs, catch query errors as `ToolError`, return JSON-serializable dicts (never raw DataFrames), log queries at DEBUG
- **NaN handling:** NaN/NaT → `None`, Timestamps → ISO date strings, Inf → `None`
- **Empty results:** return `[{"message": "..."}]` not an error
- **Compustat filter:** always apply `indfmt='INDL' AND datafmt='STD' AND consol='C' AND curcd='USD'`
- **Credentials:** env vars only (`WRDS_USERNAME`, `WRDS_PASSWORD`), never hardcoded
- **Tests:** mock `get_wrds_connection` and `resolve_ticker_to_gvkey`, never hit real WRDS API

## Tool Inventory (19 tools)

### catalog.py
- `get_data_catalog(refresh=False)` — live catalog of all datasets with date ranges and tool routing

### equity.py
- `get_stock_price_history(ticker, start_date, end_date, frequency="auto")` — CRSP daily/monthly prices
- `get_stock_returns(ticker, start_date, end_date)` — compounded cumulative + annualized return
- `get_stock_summary(ticker)` — latest price, 52-week range, market cap, YTD return

### bonds.py
- `get_bond_price_history(ticker, start_date, end_date)` — daily VWAP price/yield per CUSIP (auto-routes TRACE sources)
- `get_bond_transactions(ticker, start_date, end_date)` — individual TRACE trades
- `get_bond_yield_history(cusip, start_date, end_date)` — yield time series for a specific bond
- `get_company_bonds(ticker)` — outstanding bonds from FISD
- `get_bond_returns(ticker, start_date, end_date)` — monthly return/yield/spread/duration from bondret
- `get_bond_covenants(ticker)` — protective covenants, call/put schedules, sinking funds from FISD

### ratings.py
- `get_credit_ratings(ticker)` — current S&P/Moody's/Fitch ratings (bondret primary, Compustat fallback)
- `get_ratings_history(ticker, start_date, end_date)` — multi-agency rating changes over time

### financials.py
- `get_leverage_metrics(ticker, periods=5)` — debt/EBITDA, net debt/EBITDA
- `get_coverage_ratios(ticker, periods=5)` — interest coverage, FCC
- `get_liquidity_metrics(ticker, periods=5)` — current ratio, cash
- `get_credit_summary(ticker)` — combined: leverage + coverage + ratings + bonds + covenants + loans
- `get_company_overview(ticker)` — everything: stock performance + full credit profile

### loans.py
- `get_loan_terms(ticker)` — DealScan syndicated loan facility terms and pricing
- `get_loan_covenants(ticker)` — financial and net worth covenants on syndicated loans
