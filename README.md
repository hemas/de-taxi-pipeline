# de-taxi-pipeline

A batch ETL pipeline that ingests NYC yellow-taxi trip data, transforms it with
PySpark, and loads curated daily metrics into Google BigQuery for analytics.

Built to demonstrate the standard modern data-engineering pattern: a **processing
layer** (Spark) feeding a **warehouse** (BigQuery).

## Architecture

```
NYC TLC taxi data  ->  AWS S3  ->  PySpark  ->  BigQuery  ->  analytics
   (raw Parquet)      (storage)  (transform)   (warehouse)     (SQL/BI)
```

Medallion layout on S3: `raw/` -> `clean/` -> `aggregate/`.

## Tech stack

- **PySpark** — distributed transformation
- **AWS S3** — raw + intermediate storage (Parquet)
- **Google BigQuery** — analytics warehouse
- **Python**, **Parquet**
- Docker, Airflow — *planned*

## Pipeline stages

| Script | Layer | What it does |
|---|---|---|
| `read_local.py` | — | Sanity-check: read raw Parquet locally |
| `read_s3.py` | raw | Confirm read from S3 (`s3a://`) |
| `clean.py` | raw -> clean | Filter invalid rows, dedup, write clean layer |
| `aggregate.py` | clean -> aggregate | Daily trips / revenue / avg fare |
| `load_bq.py` | aggregate -> local | Export aggregated data to a single CSV |

**Cleaning** drops zero/negative fares, zero-distance trips, and trips with no
passengers, then applies a date-range guard to keep only the target month.

**Deduplication** uses a `row_number()` window over a six-column natural key
(vendor, pickup/dropoff times, pickup/dropoff locations, total amount), keeping
one row per unique trip — deterministic, unlike `dropDuplicates()`.

**Aggregation** groups by day into `total_trips`, `total_revenue`, `avg_fare`.

## Data source

NYC Taxi & Limousine Commission (TLC) yellow-taxi trip records, January 2024
(~2.96M rows). Public data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Running it

Prerequisites: Python venv with PySpark, AWS credentials, gcloud/bq CLI
authenticated, an S3 bucket, and a BigQuery dataset.

```bash
# S3-touching scripts need AWS credentials in the environment
export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key)

python clean.py         # raw  -> clean   (S3)
python aggregate.py     # clean -> aggregate (S3)
python load_bq.py       # aggregate -> local CSV

# load into BigQuery (--replace overwrites the table)
bq load --autodetect --source_format=CSV --skip_leading_rows=1 --replace \
  taxi_analytics.daily_metrics out_agg/part-*.csv

# verify
bq query --use_legacy_sql=false \
  'SELECT * FROM taxi_analytics.daily_metrics ORDER BY trip_date'
```

## Data-quality note

Querying the loaded table surfaced a few out-of-range dates (rows timestamped
2002, 2009, 2023) — bad source timestamps that the initial cleaning step didn't
catch. A date-range filter between the raw and clean layers removes them, leaving
only valid rows for the target month. Caught by inspecting the pipeline's own
output.

## Roadmap

- [ ] Docker containerization
- [ ] Airflow DAG for orchestration (ingest -> clean -> aggregate -> load)
- [ ] Spark-BigQuery connector (write S3 -> BigQuery directly, remove the CSV hop)

## Note on credentials

No credentials are committed to this repo. AWS keys, the GCP service-account key,
and local environment files are excluded via `.gitignore`.
