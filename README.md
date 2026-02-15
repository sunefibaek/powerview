# powerview
Powerview is a tool to analyze electricity consumption with data from the Danish public API supplied by [Eloverblik](https://eloverblik.dk/) and can be used by anyone on the Danish Energinet grid.\
The data is collected, stored in parquet files, and prepared for analysis in a DuckDB database with relevant views on top of the Parquet files.\
An [Apache Superset](https://superset.apache.org/) configuration (docker-compose, dockerfile) is included in the project. An tool that can consume data from DuckDB or Parquet can be used.\
For full documentation go to [sunefibaek.github.io/powerview/](https://sunefibaek.github.io/powerview/).\
## Quick Start Guide
### Data collection
#### Clone the repo
```bash
git clone sunefibaek/powerview
```
#### Initial Setup
Copy .env.example to .env and insert your API key from Eloverblik. The key can be created by signing in with MitID and generating a key.\
Copy the file metering_points.yml.example to metering_points.yml and insert the required metering point IDs in the file. Fill in `name`, `type`, `location`, `description`, and `price_area` (`DK1` - which covers Jutland and Funen -  or `DK2` - covering Sealand, Lolland-falster, Møn, and Bornholm). These fields are not used directly in ingestion but are made available in the reporting layer.
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
Rename ./superset/.env.example to .env and update the passwords in the file.
Cd into the superset directory and run
```bash
docker compose up -d
```
to start the Superset docker container.
Run each of these steps to initialize Superset within the container:
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
Navigate to [http://localhost:8088/](http://localhost:8088/) and sign into Superset with the credentials created above (admin:admin if not changed).
#### Connect to DuckDB
In Superset navigate to Settings -> Database Connections -> Add Database. Select DuckDB from the dropdown menu and connect to `duckdb:////app/duckdb/analytics.duckdb`.
#### Create dashboards
Starting building dashboards.
### API Documentation
The source API has a [Swagger page](https://api.eloverblik.dk/customerapi/index.html) with further documentation.
