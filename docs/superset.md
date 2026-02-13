# Superset

Superset runs via Docker Compose in the superset/ directory.

## Setup

1) Copy the example env file and set a password.

```bash
cp superset/.env.example superset/.env
```

2) Start containers.

```bash
cd superset
docker compose up -d
```

3) Initialize the Superset database and admin user.

```bash
docker compose exec superset superset db upgrade
docker compose exec superset superset fab create-admin \
	--username admin \
	--firstname Admin \
	--lastname User \
	--email admin@superset.com \
	--password admin
docker compose exec superset superset init
```

## Access

Open http://localhost:8088 and sign in with the credentials you created.

## Connect to DuckDB

1) In Superset, go to **Settings → Database Connections → + Database**.
2) Select **DuckDB**.
3) Use the SQLAlchemy URI below.

```
duckdb:////app/duckdb/analytics.duckdb
```

If you want to query Parquet directly, use:

```
duckdb:////app/external_data
```

## Environment

Check superset/.env for settings used by Docker Compose.
