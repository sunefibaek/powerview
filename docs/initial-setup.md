# Initial setup

Initial setup requires an API key from Eloverblik. Sign in with MitID, open **API Access** (top-right menu), click **Create token**, give it a name, and copy the token. You will only be able to see the key once, after closing the modal.

## 1) Configure environment variables

Copy the example env file and add your token:

```
cp .env.example .env
```

Set the token in `.env`:

```
ELOVERBLIK_REFRESH_TOKEN=...your token...
```

Optional settings (defaults shown):

- `DATA_STORAGE_PATH=./data`
- `ANALYTICS_DB_PATH=./duckdb/analytics.duckdb`
- `STATE_DB_PATH=./state.duckdb`
- `INITIAL_BACKFILL_DAYS=1095`

## 2) Define metering points

Copy the sample file and register the metering point IDs you want to track:

```
cp metering_points.yml.example metering_points.yml
```

Fill in `name`, `type`, `location`, and `description`. These fields are used for labeling in the reporting layer.

The Danish energy market is separated into two independant bidding zones or price areas: DK1 (West) and DK2 (East). Set `price_area` to `DK1` or `DK2` for each metering point depending on the location they are in. `DK1` covers Jutland and Funen. `DK2` covers Sealand, Lolland-falster, Møn, and Bornholm.

Example:

```yaml
metering_points:
	"123234345":
		name: Home Consumption
		type: consumption
		location: Home
		description: Main household consumption
		price_area: DK1
```

## 3) Install dependencies

The project uses Poetry:

```
poetry install
```

## 4) Run the ingestion pipeline

Run the main module to fetch data and store Parquet files in `./data`:

```
poetry run python -m powerview.src.main
```

The first run performs a backfill based on `INITIAL_BACKFILL_DAYS`, then future runs incrementally ingest new data.
