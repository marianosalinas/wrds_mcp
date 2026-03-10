# wrds-mcp

An MCP server that gives AI assistants (Claude Code, Claude Desktop, etc.) natural language access to [WRDS](https://wrds-www.wharton.upenn.edu/) financial data for credit and equity analysis.

**29 tools** across bonds, credit ratings, financials, equity, loans, screening, and comps — plus a guarded SQL escape hatch and schema discovery for ad-hoc analysis.

> **Requires a WRDS account.** This tool is a client for [WRDS (Wharton Research Data Services)](https://wrds-www.wharton.upenn.edu/). You must have your own WRDS subscription (academic or institutional) to use it. This project is not affiliated with, endorsed by, or sponsored by WRDS or the Wharton School.

## Prerequisites

- Python 3.10+
- A [WRDS](https://wrds-www.wharton.upenn.edu/) account with access to the datasets you want to query (CRSP, Compustat, TRACE, FISD, DealScan, etc.)

## Installation

```bash
pip install wrds-mcp
```

Or from GitHub:

```bash
pip install git+https://github.com/marianosalinas/wrds-mcp.git
```

For development:

```bash
git clone https://github.com/marianosalinas/wrds-mcp.git
cd wrds-mcp
pip install -e ".[dev]"
```

## Configuration

### 1. WRDS Credentials

Create a `.env` file in your working directory or set environment variables:

```bash
WRDS_USERNAME=your_username
WRDS_PASSWORD=your_password
```

Or copy the included example:

```bash
cp .env.example .env
```

### 2. Add to Claude Code

Create a `.mcp.json` in your project directory:

```json
{
  "mcpServers": {
    "wrds": {
      "command": "wrds-mcp",
      "type": "stdio"
    }
  }
}
```

Or add to your global Claude Code settings (`~/.claude/settings.json`):

```json
{
  "mcpServers": {
    "wrds": {
      "command": "wrds-mcp"
    }
  }
}
```

### 3. Add to Claude Desktop

Add to your Claude Desktop config (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "wrds": {
      "command": "wrds-mcp"
    }
  }
}
```

## Tools

### Bonds (TRACE / FISD / bondret)

| Tool | Description |
|------|-------------|
| `get_bond_price_history` | Daily VWAP price/yield per CUSIP — auto-routes between TRACE Enhanced and raw TRACE, with 144A fallback |
| `get_bond_transactions` | Individual TRACE trades with price, yield, volume |
| `get_bond_yield_history` | Yield time series for a specific CUSIP |
| `get_company_bonds` | All outstanding bonds from FISD (coupon, maturity, seniority, covenants) |
| `get_bond_returns` | Monthly return, yield, spread, duration from bondret |
| `get_bond_covenants` | Protective covenants, call/put schedules, sinking funds |

### Credit Ratings (bondret + Compustat)

| Tool | Description |
|------|-------------|
| `get_credit_ratings` | Current S&P, Moody's, and Fitch ratings |
| `get_ratings_history` | Multi-agency rating changes over time |

### Financial Metrics (Compustat)

| Tool | Description |
|------|-------------|
| `get_leverage_metrics` | Debt/EBITDA, net debt/EBITDA, total debt trends |
| `get_coverage_ratios` | Interest coverage, fixed charge coverage |
| `get_liquidity_metrics` | Current ratio, cash, short-term investments |
| `get_quarterly_leverage` | Quarterly debt/TTM-EBITDA trending |
| `get_credit_summary` | Combined snapshot: leverage + coverage + ratings + bonds + covenants + loans |
| `get_company_overview` | Everything: stock performance + full credit profile |

### Equity (CRSP)

| Tool | Description |
|------|-------------|
| `get_stock_price_history` | Daily/monthly prices with auto-frequency selection |
| `get_stock_returns` | Compounded cumulative + annualized returns |
| `get_stock_summary` | Latest price, 52-week range, market cap, YTD return |

### Syndicated Loans (DealScan)

| Tool | Description |
|------|-------------|
| `get_loan_terms` | Facility details, pricing, maturity |
| `get_loan_covenants` | Financial and net worth covenants |

### Screening & Relative Value (bondret)

| Tool | Description |
|------|-------------|
| `screen_issuers` | Find issuers by rating, leverage, sector, market cap |
| `screen_bonds` | Find bonds by spread, yield, coupon, maturity, rating |
| `get_market_benchmarks` | Monthly index-style returns for IG/HY/rating buckets |
| `get_relative_value` | Compare issuer bonds vs rating-peer averages (cheap/rich/fair) |

### Comps (multi-source)

| Tool | Description |
|------|-------------|
| `get_comps_table` | Side-by-side comparison of up to 20 issuers: ratings, financials, bond stats, equity returns. Falls back to FISD for 144A issuers not in bondret. |

### Discovery & Ad-hoc

| Tool | Description |
|------|-------------|
| `get_data_catalog` | Live catalog of all datasets with date coverage and tool routing |
| `get_table_schema` | Column metadata with types and human-readable descriptions |
| `resolve_identifier` | Resolve ticker to gvkey, permno, or FISD issuer_id |
| `query_wrds` | Guarded read-only SQL for anything not covered by curated tools (schema-allowlisted, 10K row limit, 30s timeout) |

## Example Prompts

Once configured, just ask Claude naturally:

- *"What's Ford's credit profile? Include ratings, leverage, and bond spreads."*
- *"Compare F, GM, and STLA — build me a comps table."*
- *"Screen for BB-rated issuers with leverage under 4x and market cap over $5B."*
- *"Show me the HY market benchmark returns for the last 12 months."*
- *"Is Ford's 2030 bond cheap or rich relative to BBB peers?"*
- *"What are the covenant terms on Delta's syndicated loans?"*
- *"Pull TRACE transactions for Apple bonds in Q4 2024."*

## Data Sources

| Source | Tables | Coverage |
|--------|--------|----------|
| **CRSP** | `dsf_v2`, `msf_v2` | Daily/monthly stock data |
| **TRACE** | `trace`, `trace_enhanced_clean`, `trace_btds144a` | Bond transactions (raw, cleaned, 144A) |
| **bondret** | `wrdsapps_bondret.bondret` | Monthly bond returns, ratings, spreads |
| **FISD** | `fisd_mergedissue`, `fisd_mergedissuer`, covenants/call/put/sink | Bond characteristics |
| **Compustat** | `funda`, `fundq`, `security`, `adsprate` | Financials, ticker resolution, ratings |
| **DealScan** | `facility`, `package`, `borrower`, `company`, `currfacpricing`, covenants | Syndicated loans |

## Architecture

```
src/wrds_mcp/
├── server.py             # FastMCP entry point, mounts 9 sub-servers
├── db/connection.py      # Singleton WRDS connection with retry
└── tools/
    ├── catalog.py        # Tier 3: discovery (3 tools)
    ├── query.py          # Tier 2: guarded SQL (1 tool)
    ├── equity.py         # Tier 1: CRSP stocks (3 tools)
    ├── bonds.py          # Tier 1: TRACE/FISD/bondret (6 tools)
    ├── ratings.py        # Tier 1: credit ratings (2 tools)
    ├── financials.py     # Tier 1: Compustat fundamentals (6 tools)
    ├── loans.py          # Tier 1: DealScan loans (2 tools)
    ├── screening.py      # Tier 1: screening & relative value (4 tools)
    └── comps.py          # Tier 1: comps table (1 tool)
```

**Three tiers:**
1. **Curated tools** (25) — domain logic, auto-routing, multi-source resolution
2. **Guarded SQL** (1) — escape hatch for ad-hoc queries with safety validation
3. **Discovery** (3) — catalog, schema introspection, identifier resolution

## Testing

```bash
pytest
```

307 tests, 90% coverage. All tests mock the WRDS connection — no credentials needed to run them.

## Disclaimer

This project is an independent, open-source tool. It is **not affiliated with, endorsed by, or sponsored by WRDS, the Wharton School, or the University of Pennsylvania**.

- You are responsible for complying with your WRDS subscription terms and any data redistribution restrictions.
- This tool provides read-only access to data you are already licensed to use — it does not bypass any access controls.
- Nothing in this tool constitutes financial advice. Data is provided as-is for research and analysis purposes.

## License

MIT
