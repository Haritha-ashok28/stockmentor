# Stock Mentor

A pocket mentor for people new to investing. Search a stock, and Stock Mentor explains, in plain English, whether the company is financially healthy, how it's priced, and what's happening around it lately.

Built end to end as a production-style data engineering project: ingestion, validation, transformation, and serving, on a modern lakehouse-style stack.

---

## The problem

Most stock data is built for people who already know what P/E, EPS, or debt-to-equity mean. Stock Mentor is for people who don't, yet. It takes a beginner from "I just know the company name" to "I understand whether this is a reasonable stock to look at," without needing a finance background.

For any of the 50 tickers tracked, the app shows:

- **A 9-point financial health check:** profitability, valuation, risk, shareholder return, and growth, each explained in plain language
- **KPI cards:** price, P/E, revenue, EPS, with tap-to-expand explanations
- **Recent news:** headline, summary, source, and link
- **Search with filters:** by company name/ticker, sector, or investment budget

---

## Roadmap

This is the one section updated as the project progresses, phase by phase.

**Phase 1: Data engineering foundation** *(current)*
- [x] Ingestion scripts: Yahoo Finance, SEC EDGAR, RSS news
- [x] Bronze schema and partitioning strategy
- [x] Source profiling and known-issue documentation
- [x] Great Expectations validation layer
- [ ] Airflow DAGs (3, one per source)
- [ ] dbt silver models (cleaning, standardization, unit conversion)
- [ ] dbt gold models (health score, KPIs, company lookup)
- [ ] Supabase serving layer
- [ ] Lovable frontend integration

**Phase 2: Intelligence layer** *(planned)*
- [ ] LLM-powered chat: ask free-form questions about any tracked stock
- [ ] News sentiment analysis
- [ ] Expand beyond 50 tickers to full Yahoo Finance universe

---

## Architecture

Five layers, each with a clear job. Sources feed bronze; everything downstream is built from there.

```
        Sources                Bronze              Silver             Gold              Serving        Frontend
   ┌───────────────┐      ┌──────────────┐    ┌──────────────┐  ┌──────────────┐   ┌─────────────┐  ┌──────────┐
   │ Yahoo Finance │      │              │    │              │  │              │   │             │  │          │
   │ SEC EDGAR     │ ───► │   Parquet    │───►│ Clean +      │─►│ Health score │──►│  Supabase   │─►│ Lovable  │
   │ RSS News      │      │ (raw, as-is) │    │ standardize  │  │ KPIs, news   │   │  Postgres   │  │          │
   └───────────────┘      └──────────────┘    └──────────────┘  └──────────────┘   └─────────────┘  └──────────┘
                                  │                   ▲
                                  └── Great Expectations
                                      (validates before silver)
```

| Layer | Tool | Job |
|---|---|---|
| Sources | `yfinance`, `edgartools`, `feedparser` | Fetch raw data from each API |
| Bronze | Parquet on Supabase Storage | Store exactly what each source returned, no transformation |
| Validation | Great Expectations | Catch bad data before it moves downstream |
| Silver | DuckDB + dbt | Clean, standardize units, normalize formats |
| Gold | DuckDB + dbt | Business logic: health score, KPIs, company lookup |
| Serving | Supabase Postgres | Final tables, auto-generated REST API |
| Frontend | Lovable | The app itself |

Orchestrated by **Apache Airflow** across three independent DAGs, one per source, since each has a different natural refresh cadence:

| DAG | Source | Schedule | Notes |
|---|---|---|---|
| Prices | Yahoo Finance | Weekdays, 5pm ET | Backfills 2 years on first run, incremental after |
| Financials | SEC EDGAR | Weekly | Incremental, only fetches if a new filing exists since last check |
| News | Yahoo Finance RSS | Every 4 hours | Merges with same-day data, deduplicates by article id |

### Why this stack

| Choice | Instead of | Why |
|---|---|---|
| Parquet bronze | Writing straight to a DB | Unreliable upstream APIs (Yahoo, SEC rate limits); parquet gives a safe, replayable raw snapshot |
| DuckDB | Pandas / Spark | Out-of-core SQL on parquet, no infrastructure, scales well past 50 tickers without the overhead of distributed compute |
| dbt | Raw Python/SQL scripts | Built-in lineage, testing, and documentation; all business logic lives in one place, version-controlled |
| Supabase | Plain Postgres / Firebase | Postgres (relational, fits financial data) + auto-generated REST API + storage, all in one platform |
| Airflow | Cron / GitHub Actions | DAG-based dependency management, retries, monitoring; industry standard |

---

## Data model

Star schema. `company_lookup` is the central dimension; everything else is a fact table joined to it via `ticker`.

| Table | Grain | Strategy | Powers |
|---|---|---|---|
| `company_lookup` | one row per ticker | overwrite (SCD Type 1) | search, filters, company header |
| `health_score` | one row per ticker | overwrite (SCD Type 1) | 9-point health check, KPI cards, summary |
| `price_history` | one row per ticker per date | append-only | price chart, trend context |
| `news` | one row per article | append-only | news feed |
| `metadata` | one row per ticker per source | overwrite (SCD Type 1) | pipeline state: last fetch time, status, retries |

### The 9-point health check

| # | Check | Metric | Source |
|---|---|---|---|
| 1 | Profitability | Net profit margin | SEC income statement |
| 2 | Capital efficiency | Return on equity | SEC income + balance sheet |
| 3 | Valuation | P/E ratio | SEC earnings + Yahoo price |
| 4 | Debt risk | Debt-to-equity | SEC balance sheet |
| 5 | Liquidity | Current ratio | SEC balance sheet |
| 6 | Shareholder return | EPS growth (YoY) | SEC income statement |
| 7 | Income | Pays dividend | Yahoo Finance |
| 8 | Growth trend | Revenue growth (trailing 4 quarters) | SEC income statement |
| 9 | Cash quality | Free cash flow | SEC cash flow statement |

---

## Source notes

Each source required different handling, documented here so the reasoning isn't lost:

**Yahoo Finance:** clean, computed ratios, but several fields are percentage-scaled (`debtToEquity`, `returnOnEquity`) rather than raw ratios, and `marketCap`/`totalRevenue` need `bigint` storage to avoid overflow. Exchange codes (`NMS`) need mapping to readable names (`NASDAQ`).

**SEC EDGAR** (via `edgartools`): raw regulatory filings, not analyst-ready data. Values are reported in millions (except EPS and share counts, which are raw). No single "total debt" field; it's the sum of commercial paper and term debt (current + non-current). No "free cash flow" field; calculated as operating cash flow minus capex. Statements come back wide (one column per fiscal period); melted to long format in bronze to make partitioning by fiscal year possible.

**News:** originally used Google News RSS, but its `summary` field is just the headline re-wrapped in HTML, not real article content, and its `link` is a Google redirect rather than the publisher's actual URL. Switched to Yahoo Finance's ticker-specific RSS feed, which provides genuine article excerpts and direct publisher links.

---

## Tech stack

- **Ingestion:** Python (`yfinance`, `edgartools`, `feedparser`)
- **Storage:** Parquet, Supabase Storage
- **Validation:** Great Expectations
- **Orchestration:** Apache Airflow
- **Processing:** DuckDB
- **Transformation:** dbt
- **Serving:** Supabase (Postgres + auto REST API)
- **Frontend:** Lovable

---

## Why this project

Built to learn production-grade data engineering hands-on: medallion architecture, source profiling, data modeling, orchestration, and the tradeoffs behind each tool choice, while building something real enough to use and explain end to end.
