# wrds-mcp

MCP server providing Claude Code with natural language access to WRDS financial data for credit and equity analysis.

## Quick Reference

- **Run tests:** `python -m pytest tests/ -v`
- **Run server:** `wrds-mcp` or `fastmcp run src/wrds_mcp/server.py:mcp`
- **Install dev:** `pip install -e ".[dev]"`

## Architecture

Three-tier design: curated tools handle domain-specific logic, `query_wrds` provides guarded SQL access for ad-hoc analysis, and catalog tools enable schema discovery.

```
src/wrds_mcp/
├── server.py             # FastMCP entry point, mounts 9 sub-servers
├── db/connection.py      # Singleton WRDS connection with retry (3 attempts, exponential backoff)
└── tools/
    ├── _validation.py    # Input validation + DataFrame-to-JSON conversion
    ├── _schema_docs.py   # Static column documentation (~95 WRDS mnemonics)
    ├── catalog.py        # Tier 3: discovery — live catalog, schema introspection, ID resolution (3 tools)
    ├── query.py          # Tier 2: guarded SQL — read-only SELECT with safety validation (1 tool)
    ├── equity.py         # Tier 1: CRSP stock tools — price history, returns, summary (3 tools)
    ├── bonds.py          # Tier 1: TRACE/FISD/bondret — transactions, prices, returns, covenants (6 tools)
    ├── ratings.py        # Tier 1: Credit ratings from bondret + Compustat fallback (2 tools)
    ├── financials.py     # Tier 1: Compustat leverage/coverage/liquidity + composites (6 tools)
    ├── loans.py          # Tier 1: DealScan syndicated loan terms + covenants (2 tools)
    ├── screening.py      # Tier 1: Issuer/bond screening, benchmarks, relative value (4 tools)
    └── comps.py          # Tier 1: Side-by-side comps table (1 tool)
```

### Three Tiers

1. **Tier 1 — Curated Tools** (25 tools): Pre-built tools with domain logic (auto-routing, TTM calculations, multi-source resolution). Use these first.
2. **Tier 2 — `query_wrds`** (1 tool): Guarded SQL escape hatch for any metric not covered by Tier 1. SELECT-only, schema-allowlisted, 10K row limit, 30s timeout.
3. **Tier 3 — Discovery** (3 tools): `get_data_catalog` (what data exists), `get_table_schema` (column metadata + docs), `resolve_identifier` (ticker → gvkey/permno/issuer_id).

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
| `comp.fundq` | Quarterly financial fundamentals (TTM calculations) |
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

- **Three-tier architecture:** Curated tools for complex domain logic → guarded SQL for ad-hoc queries → schema discovery for self-service exploration
- **Auto-routing:** Bond price/yield tools auto-route between `trace_enhanced_clean` (historical, research quality) and `trace.trace` (recent, raw) based on whether end_date is within the last ~12 months
- **144A fallback:** When standard TRACE tables return empty, bond tools automatically check `trace.trace_btds144a` for 144A private placements (e.g., PRKS/SeaWorld bonds)
- **Discovery tools:** `get_data_catalog()` queries live date ranges; `get_table_schema()` returns column metadata with human-readable docs; `resolve_identifier()` bridges ticker → WRDS IDs
- **Guarded SQL:** `query_wrds` validates queries (SELECT-only, no mutation keywords, schema allowlist), enforces LIMIT 10K and 30s timeout, warns about missing Compustat filters
- **Ratings primary source:** `wrdsapps_bondret.bondret` provides S&P + Moody's + Fitch through latest month; `comp.adsprate` is fallback only (ended Feb 2017)
- **FISD ticker matching:** Uses issuer_id subquery pattern because many bonds have NULL ticker in fisd_mergedissue. Falls back to Compustat CUSIP linkage (5-char prefix match) when FISD has no ticker at all
- **TTM EBITDA:** `get_quarterly_leverage` computes rolling 4-quarter sum from `comp.fundq` for accurate net leverage trending

## Conventions

- **Commits:** conventional commits (`feat:`, `fix:`, `test:`, `docs:`)
- **All tools:** validate inputs, catch query errors as `ToolError`, return JSON-serializable dicts (never raw DataFrames), log queries at DEBUG
- **NaN handling:** NaN/NaT → `None`, Timestamps → ISO date strings, Inf → `None`
- **Empty results:** return `[{"message": "..."}]` not an error
- **Compustat filter:** always apply `indfmt='INDL' AND datafmt='STD' AND consol='C' AND curcd='USD'`
- **Credentials:** env vars only (`WRDS_USERNAME`, `WRDS_PASSWORD`), never hardcoded
- **Tests:** mock `get_wrds_connection` and `resolve_ticker_to_gvkey`, never hit real WRDS API
- **Allowed schemas:** comp, crsp, trace, wrdsapps_bondret, fisd, dealscan

## Tool Inventory (29 tools)

### catalog.py (Tier 3 — Discovery)
- `get_data_catalog(refresh=False)` — live catalog of all datasets with date ranges and tool routing
- `get_table_schema(schema, table)` — column metadata with types, nullability, and human-readable descriptions
- `resolve_identifier(ticker, target)` — resolve ticker to gvkey, permno, or issuer_id

### query.py (Tier 2 — Guarded SQL)
- `query_wrds(sql, params=None)` — execute read-only SELECT against WRDS with safety validation

### equity.py (Tier 1)
- `get_stock_price_history(ticker, start_date, end_date, frequency="auto")` — CRSP daily/monthly prices
- `get_stock_returns(ticker, start_date, end_date)` — compounded cumulative + annualized return
- `get_stock_summary(ticker)` — latest price, 52-week range, market cap, YTD return

### bonds.py (Tier 1)
- `get_bond_price_history(ticker, start_date, end_date)` — daily VWAP price/yield per CUSIP (auto-routes TRACE sources)
- `get_bond_transactions(ticker, start_date, end_date)` — individual TRACE trades
- `get_bond_yield_history(cusip, start_date, end_date)` — yield time series for a specific bond
- `get_company_bonds(ticker)` — outstanding bonds from FISD
- `get_bond_returns(ticker, start_date, end_date)` — monthly return/yield/spread/duration from bondret
- `get_bond_covenants(ticker)` — protective covenants, call/put schedules, sinking funds from FISD

### ratings.py (Tier 1)
- `get_credit_ratings(ticker)` — current S&P/Moody's/Fitch ratings (bondret primary, Compustat fallback)
- `get_ratings_history(ticker, start_date, end_date)` — multi-agency rating changes over time

### financials.py (Tier 1)
- `get_leverage_metrics(ticker, periods=5)` — debt/EBITDA, net debt/EBITDA
- `get_coverage_ratios(ticker, periods=5)` — interest coverage, FCC
- `get_liquidity_metrics(ticker, periods=5)` — current ratio, cash
- `get_quarterly_leverage(ticker, quarters=12)` — quarterly debt/TTM EBITDA trending
- `get_credit_summary(ticker)` — combined: leverage + coverage + ratings + bonds + covenants + loans
- `get_company_overview(ticker)` — everything: stock performance + full credit profile

### loans.py (Tier 1)
- `get_loan_terms(ticker)` — DealScan syndicated loan facility terms and pricing
- `get_loan_covenants(ticker)` — financial and net worth covenants on syndicated loans

### screening.py (Tier 1)
- `screen_issuers(rating_class, min_rating, max_rating, sector, min_market_cap, ...)` — find issuers by credit, financial, and sector criteria
- `screen_bonds(ticker, rating_class, security_level, min_amount_outstanding, ...)` — find bonds by rating, spread, coupon, maturity, sector
- `get_market_benchmarks(start_date, end_date, rating_class, rating_category)` — monthly index-style returns (EW + VW spread, yield, return) for IG/HY/rating buckets
- `get_relative_value(ticker)` — compare issuer's bonds vs rating-peer averages (spread, yield, percentiles, cheap/rich/fair)

### comps.py (Tier 1)
- `get_comps_table(tickers)` — side-by-side credit comps: ratings, leverage, coverage, market cap, bond stats, equity returns for up to 20 tickers
