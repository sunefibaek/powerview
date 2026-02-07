# Data and storage

## Local data layout

Data is stored under the top-level data/ directory with partitions for metering points and dates.

## DuckDB

DuckDB files are stored at the repository root and under duckdb/.

- analytics.duckdb
- state.duckdb

Use create_analytics_db.py to (re)build analytics data.
