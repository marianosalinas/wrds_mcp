# wrds-mcp

MCP server that gives Claude Code natural language access to WRDS financial data for credit analysis.

## Setup

1. **Install dependencies:**
   ```bash
   pip install -e ".[dev]"
   ```

2. **Configure credentials:**
   ```bash
   cp .env.example .env
   # Edit .env with your WRDS username and password
   ```

3. **Run the server:**
   ```bash
   wrds-mcp
   # or: fastmcp run src/wrds_mcp/server.py:mcp
   ```

4. **Add to Claude Code** (`~/.claude/settings.json`):
   ```json
   {
     "mcpServers": {
       "wrds": {
         "command": "wrds-mcp"
       }
     }
   }
   ```

## Tools Reference

### Bonds (TRACE / FISD)

| Tool | Description |
|------|-------------|
| `get_bond_transactions(ticker, start_date, end_date)` | TRACE transaction-level bond data |
| `get_bond_yield_history(cusip, start_date, end_date)` | Daily yield time series for a specific bond |
| `get_company_bonds(ticker)` | All outstanding bonds with coupon, maturity, seniority |

### Credit Ratings (Compustat)

| Tool | Description |
|------|-------------|
| `get_credit_ratings(ticker)` | Most recent S&P issuer credit rating |
| `get_ratings_history(ticker, start_date, end_date)` | Rating changes over time |

### Financial Metrics (Compustat)

| Tool | Description |
|------|-------------|
| `get_leverage_metrics(ticker, periods=5)` | Debt/EBITDA, net debt/EBITDA, total debt |
| `get_coverage_ratios(ticker, periods=5)` | Interest coverage, fixed charge coverage |
| `get_liquidity_metrics(ticker, periods=5)` | Current ratio, cash, short-term investments |
| `get_credit_summary(ticker)` | Combined credit snapshot (leverage + coverage + ratings + bonds) |

## Testing

```bash
pytest
```

All tests mock the WRDS connection and never hit the real API.

## Data Sources

- **TRACE Enhanced** (`trace.trace_enhanced`) — Bond transaction data
- **FISD** (`fisd.fisd_mergedissue`, `fisd.fisd_mergedissuer`) — Bond characteristics
- **Compustat Annual** (`comp.funda`) — Financial fundamentals
- **Compustat Ratings** (`comp.adsprate`) — S&P ratings (through Feb 2017)
