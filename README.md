# powerview
Powerview is a tool to analyze electricity consumption with data from the Danish public API supplied by [Eloverblik]]https://eloverblik.dk/) and can be used by anayone on the Danish Energinet grid.\
The data is collected, stored in parquet files, and prepared for analysis with [Apache Superset](https://superset.apache.org/) in a [DuckDB](https://duckdb.org/) database.
## Quick Start Guide
See the full guide for detailed setup.
### Data collection
#### Clone the repo
```bash
git clone sunefibaek/powerview
```
#### Initial Setup
Copy .env.sample to .env and insert your API key from Eloverblik. The key can be created by signing in with MitID and generating a key.\
Copy the file metering_points.example to meterings_points.yml and insert the required metering point IDs in the file. Fill in `name`, `type`, `location`, and `description`. These fields are not used directly but are made available in the reporting layer.
#### Backfill
The first run is used for back filling data. The amount of data fetched initially is defaulted to 90 days. The value can be changed in `.env` in the `INITIAL_BACKFILL_DAYS=90` parameter. The API has a cap of 730 days.\
The run might take a fair bit of time depending on the amount of days.
#### Install requirements
The projects uses Poetry for package management.
```bash
poetry install
```
#### Run
Run the scrip
```bash
poetry run python -m powerview.src.main
```
The script will fetch data and store it in Parquet files in the `./data` folder. If the folder does not exist the script will create it.
### Analytics
#### Analytics database
To set up the analytics database run
```bash
poetry run python create_analytics_db.py
```
This will create `analytics.duckdb` in `./duckdb`.
#### Initialize Superset
Rename ./superset/.env.example to .env and update the passqwords in the file.
Cd into the superset directory and run
```bash
docker compose up -d
```
to start the Superset docker container.
Run each of these steps to initialize superset within the container:
```bash
# Initialize the database
docker exec -it superset superset db upgrade
```
```bash
# Create an admin user (replace credentials as needed)
docker exec -it superset superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@superset.com \
  --password admin
```
```
# Initialize Superset
docker exec -it superset superset init
```
Navigate to [http://localhost:8088/](http://localhost:8088/) and sign into superset with the credentials created above (admin:admin if not changed).
#### Connect to DuckDB
IN superset navigate to settings -> database connections -> add database. Select DuckDB on the dropdown and connect to `duckdb:////app/duckdb/analytics.duckdb`.
#### Create dashboards
Starting building dashboards.
### API Documentation
The source API has a [Swagger page](https://api.eloverblik.dk/customerapi/index.html) with further documentation.
