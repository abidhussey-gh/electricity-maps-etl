# Electricity Maps ETL Pipeline

A production-grade Python/PySpark ETL pipeline implementing the **Bronze → Silver → Gold** medallion architecture to analyse France's electricity flows and production mix using the [Electricity Maps API](https://www.electricitymaps.com/).

Built for the NXP Semiconductors HDA team technical assignment.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Project Structure](#project-structure)
3. [Installation & Environment Setup](#installation--environment-setup)
4. [How to Run the Pipeline](#how-to-run-the-pipeline)
5. [Design Decisions](#design-decisions)
6. [Schema Descriptions](#schema-descriptions)
7. [Data Quality Checks](#data-quality-checks)
8. [Incremental Ingestion](#incremental-ingestion)
9. [Running Tests](#running-tests)
10. [CI/CD](#cicd)
11. [Bonus Features Implemented](#bonus-features-implemented)

---

## Architecture Overview

```
Electricity Maps API (sandbox)
          │
          ▼
┌─────────────────────────────────────────────────────┐
│  BRONZE  │  Raw JSON  │  Partitioned by ingestion ts │
│          │  + envelope metadata                      │
└────────────────────────┬────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────┐
│  SILVER  │  Parquet + Delta Lake                     │
│          │  Flattened, typed, deduplicated           │
│          │  Partitioned by data timestamp            │
└────────────────────────┬────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────┐
│  GOLD    │  Parquet + Delta Lake                     │
│  DP 1    │  Daily Relative Electricity Mix           │
│  DP 2    │  Daily Net Import / Export (MWh)          │
└─────────────────────────────────────────────────────┘
```

Two data streams are ingested independently:

| Stream | Source endpoint | Description |
|---|---|---|
| `electricity_mix` | `/power-breakdown/history` | Hourly consumption breakdown per energy source |
| `electricity_flows` | `/power-breakdown/history` | Hourly import/export flows per trading partner |

---

## Project Structure

```
electricity-maps-etl/
├── pipeline.py                      # Main entry point
├── config/
│   └── settings.py                  # Centralised configuration
├── src/
│   ├── bronze/
│   │   ├── ingest_electricity_mix.py
│   │   └── ingest_electricity_flows.py
│   ├── silver/
│   │   ├── transform_electricity_mix.py
│   │   └── transform_electricity_flows.py
│   ├── gold/
│   │   ├── build_daily_mix.py       # Data Product 1
│   │   └── build_net_flows.py       # Data Product 2
│   └── utils/
│       ├── api_client.py            # Retry-aware API wrapper
│       ├── spark_session.py         # SparkSession factory
│       ├── watermark.py             # Incremental state tracker
│       └── data_quality.py          # DQ checks
├── tests/
│   ├── test_bronze.py
│   ├── test_silver.py
│   ├── test_api_client.py
│   └── test_data_quality.py
├── .github/
│   └── workflows/
│       └── ci.yml                   # GitHub Actions CI/CD
├── requirements.txt
├── pytest.ini
├── .env.example
└── .gitignore
```

---

## Installation & Environment Setup

### Prerequisites

- Python 3.10+
- Java 11+ (required for PySpark/Spark)
- Git

### 1. Clone the repository

```bash
git clone https://github.com/abidhussey-gh/electricity-maps-etl.git
cd electricity-maps-etl
```

### 2. Create a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate        # Linux / macOS
# .venv\Scripts\activate         # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt 
```

> **Note:** `delta-spark` downloads Spark JARs on first run — this may take a minute.

### 4. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` and set your Electricity Maps sandbox API key:

```ini
ELECTRICITY_MAPS_API_KEY=your_sandbox_api_key_here
DATA_LAKE_ROOT=./data          # local path; change to s3://bucket/prefix for S3
```

Get your free sandbox key at: https://help.electricitymaps.com/en/articles/13169368-using-a-sandbox-api-key

---

## How to Run the Pipeline

### Full pipeline (Bronze → Silver → Gold)

```bash
python pipeline.py
```

### Full load (ignore watermarks, re-ingest all available history)

```bash
python pipeline.py --full-load
```

### Run a specific layer only

```bash
python pipeline.py --layer bronze
python pipeline.py --layer silver
python pipeline.py --layer gold
```

### Expected output structure

```
data/
├── bronze/
│   ├── electricity_mix/
│   │   └── year=2024/month=01/day=15/
│   │       └── electricity_mix_20240115T120000Z.json
│   └── electricity_flows/
│       └── year=2024/month=01/day=15/
│           └── electricity_flows_20240115T120000Z.json
├── silver/
│   ├── electricity_mix/           ← Delta Lake table
│   ├── electricity_mix_parquet/   ← Parquet snapshot
│   ├── electricity_flows/
│   └── electricity_flows_parquet/
└── gold/
    ├── daily_electricity_mix/
    ├── daily_electricity_mix_parquet/
    ├── electricity_imports/
    ├── electricity_imports_parquet/
    ├── electricity_exports/
    └── electricity_exports_parquet/
```

---

## Design Decisions

### Framework: PySpark + Delta Lake

PySpark provides native Delta Lake integration via `delta-spark`, making ACID upserts (merge) straightforward. Delta Lake's transaction log enables reliable schema evolution, time-travel, and efficient incremental reads — this is important for a production data lake.

### Bronze: Raw JSON with envelope metadata

The Bronze layer stores API responses verbatim as JSON, enriched only with an ingestion envelope (`ingestion_timestamp`, `source_url`, `record_count`). No schema is enforced here — this preserves full fidelity of the source so any downstream issues can always be re-processed from the original data. Partitioned by **ingestion timestamp**.

### Silver: Flatten → Typed → Dedup → Upsert

The nested `powerConsumptionBreakdown` map is exploded into individual rows (one per energy source), making the data easily queryable without JSON parsing in downstream queries. A deterministic `record_id` (`zone|datetime|source_type`) enables idempotent upserts via Delta `MERGE`, so re-running the pipeline never produces duplicate rows. Partitioned by **data timestamp**.

### Gold: Aggregated, business-ready tables

Gold tables are computed by daily aggregation over Silver. It  contains refined, analytics-ready datasets built on top of Silver. Both Gold products are partitioned by `year/month` — daily partitioning would create too many small files at this data volume.

### Incremental ingestion via watermarks

A lightweight JSON watermark file (`./_watermark.json`) is persisted alongside each Bronze stream. On each pipeline run, only records newer than the watermark are fetched and processed. This avoids re-downloading the full history on every run.

### Error handling

The API client uses `tenacity` for exponential-backoff retry logic, covering HTTP 429 (rate limit) and transient connection errors. The maximum retry count and backoff multiplier are configurable via `Config`.

---

## Schema Descriptions

### Silver — `electricity_mix`

| Column | Type | Description |
|---|---|---|
| `record_id` | STRING | Dedup key: `zone\|datetime\|source_type` |
| `zone` | STRING | ISO zone code (e.g. `FR`) |
| `data_timestamp` | TIMESTAMP | Hourly measurement time (UTC) |
| `source_type` | STRING | Energy source (e.g. `nuclear`, `wind`) |
| `power_mw` | DOUBLE | Consumption for this source (MW) |
| `is_estimated` | STRING | Whether the value is estimated by the sandbox (SANDBOX_MODE_DATA)
| `ingestion_timestamp` | TIMESTAMP | When the Bronze record was written |
| `year` / `month` / `day` | STRING | Partition columns |

### Silver — `electricity_flows`

| Column | Type | Description |
|---|---|---|
| `record_id` | STRING | Dedup key: `zone\|datetime\|counterpart\|flow_type` |
| `zone` | STRING | Home zone (`FR`) |
| `data_timestamp` | TIMESTAMP | Hourly measurement time (UTC) |
| `counterpart_zone` | STRING | Trading partner (e.g. `DE`, `GB`) |
| `flow_type` | STRING | `import` \| `export` |
| `power_mw` | DOUBLE | Flow magnitude (MW) |
| `ingestion_timestamp` | TIMESTAMP | When the Bronze record was written |
| `year` / `month` / `day` | STRING | Partition columns |

### Gold — Data Product 1: `daily_electricity_mix`

| Column | Type | Description |
|---|---|---|
| `date` | DATE | Calendar date |
| `zone` | STRING | ISO zone code |
| `source_type` | STRING | Energy source |
| `daily_power_mwh` | DOUBLE | Sum of hourly MW (proxy for MWh) |
| `total_daily_consumption_mwh` | DOUBLE | Zone total for the day |
| `pct_contribution` | DOUBLE | Source share of total (0–100) |
| `reference_timestamp` | TIMESTAMP | Midnight UTC of the date |
| `pipeline_run_timestamp` | TIMESTAMP | Pipeline execution time |
| `year` / `month` | STRING | Partition columns |

### Gold — Data Product 2a: `electricity_imports`

| Column | Type | Description |
|---|---|---|
| `date` | DATE | Calendar date |
| `zone` | STRING | Destination zone (`FR`) |
| `counterpart_zone` | STRING | Source zone |
| `net_mwh` | DOUBLE | Net energy imported (MWh, always positive) |
| `flow_direction` | STRING | Always `import` |
| `reference_timestamp` | TIMESTAMP | Midnight UTC of the date |
| `pipeline_run_timestamp` | TIMESTAMP | Pipeline execution time |
| `year` / `month` | STRING | Partition columns |

### Gold — Data Product 2b: `electricity_exports`

Same schema as `electricity_imports`, with `flow_direction = 'export'`. `net_mwh` is the absolute value of the outbound flow.

---

## Data Quality Checks

DQ checks run automatically after each Silver and Gold transformation. Failures in **critical** checks (`not_empty`, `no_nulls_on_key_cols`, `no_duplicate_ids`) raise a `ValueError` and halt the pipeline.

| Check | Layer | Critical |
|---|---|---|
| `not_empty` | Silver, Gold | ✅ |
| `no_nulls_on_key_cols` | Silver, Gold | ✅ |
| `no_duplicate_ids` | Silver, Gold | ✅ |
| `power_non_negative` | Silver mix | ❌ (warning) |
| `pct_contribution_sum` | Gold mix | ❌ (warning) |

---

## Incremental Ingestion

Each Bronze stream maintains a watermark file at:

```
data/bronze/<stream>/._watermark.json
```

Example content:
```json
{
  "last_ingested": "2024-01-15T11:00:00+00:00",
  "stream": "electricity_mix"
}
```

On each run, only records with `datetime > last_ingested` are written to Bronze. To reset and run a full load:

```bash
python pipeline.py --full-load
```

---

## Running Tests

```bash
# All tests
pytest

# Specific module
pytest tests/test_bronze.py -v

```

---

## CI/CD

The GitHub Actions workflow (`.github/workflows/ci.yml`) runs on every push and PR:

1. **Lint** — Ruff + Black format check
2. **Unit Tests** — Full pytest suite with PySpark
3. **Smoke Test** — Runs `pipeline.py --layer bronze` end-to-end against the real sandbox API (on `main` branch only)

To enable the smoke test, add your API key as a GitHub Actions secret:
`Settings → Secrets and variables → Actions → New secret → ELECTRICITY_MAPS_API_KEY`

---

## Bonus Features Implemented

| Feature | Status | Location |
|---|---|---|
| Error handling & API rate-limit retries | ✅ | `src/utils/api_client.py` |
| Incremental ingestion logic | ✅ | `src/utils/watermark.py` |
| Unit tests for transformations | ✅ | `tests/` |
| Data quality checks | ✅ | `src/utils/data_quality.py` |
| CI/CD pipeline | ✅ | `.github/workflows/ci.yml` |
| Cloud storage (S3) | Configure `DATA_LAKE_ROOT=s3://...` in `.env` |
