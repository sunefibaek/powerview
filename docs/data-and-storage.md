# Data and storage

## Local data layout

Data is stored under the top-level data/ directory with partitions for metering points and dates.

Each partition contains one Parquet file:

```
data/
	metering_point=<METERING_POINT_ID>/
		date=<YYYY-MM-DD>/
			consumption_data.parquet
```

### Parquet schema

Each Parquet file contains hourly readings for a single metering point and a single day.
The schema is flat and corresponds to the normalized API response:

- `metering_point_id` (string): Metering point ID from Eloverblik.
- `timestamp` (timestamp): Hourly reading timestamp (UTC).
- `consumption_value` (float): Consumption value for the hour.
- `quality` (string): Quality marker from the API (if present).
- `unit` (string): Unit from the API (default `kWh`).
- `ingestion_timestamp` (timestamp): When the record was ingested (UTC).
- `ingestion_date` (date): Ingestion date (UTC).

## DuckDB

DuckDB files are stored at the repository root and under duckdb/.

- `duckdb/analytics.duckdb`: Analytics/reporting database. Built from the Parquet data and
	contains curated tables/views used for exploration and Superset dashboards. Use
	create_analytics_db.py to (re)build it.
- `state.duckdb`: Operational state database. Stores ingestion state (for example
	last ingestion date per metering point) to support incremental extraction.

### Analytics database views

The analytics database creates a reporting schema with default views:

- `reporting.meter_data_stage`: Raw hourly readings with derived date/time fields
	(date, hour, weekday, month, year) for downstream aggregations.
- `reporting.meter_data_clean`: Filters out null or negative consumption values to
	provide a clean base for metrics.
- `reporting.daily_consumption`: Daily aggregates per metering point (total, average,
	min, max, count).
- `reporting.monthly_consumption`: Monthly totals per metering point with month-over-month
	deltas and ratios for trend analysis.
- `reporting.hourly_profile`: Average hourly usage by weekday to describe typical load
	shapes.
- `reporting.missing_data_summary`: Daily expected vs actual reading counts to highlight
	missing hours.
- `reporting.load_variability`: Daily variability metrics (hourly standard deviation and
	peak usage) for volatility analysis.
- `reporting.daily_quality_flags`: Daily flags for suspicious zero-consumption days
	surrounded by non-zero readings.
- `reporting.meter_metadata`: Metering point metadata loaded from metering_points.yml
	for labeling and filtering.
- `reporting.meter_metadata_enriched`: Daily consumption joined with metadata for
	easy dashboarding.
