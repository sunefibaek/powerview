# Powerview

Powerview is a comprehensive data stack for collecting, storing, and analyzing detailed electricity consumption data from the Danish [Eloverblik](https://eloverblik.dk) platform.

Created for users who want full ownership of their energy data, Powerview extracts historical and ongoing consumption metrics, standardizes them into open formats, and provides a powerful analytics layer for visualization.

## How it works

1.  **Ingest:** A Python pipeline fetches hourly consumption data from the Eloverblik API.
2.  **Store:** Data is saved as partitioned **Parquet** files, creating an efficient, immutable data lake on your local disk.
3.  **Model:** A **DuckDB** analytics database creates curated views on top of the raw files, handling data cleaning, aggregations (daily/monthly), and enrichment.
4.  **Visualize:** An included **Apache Superset** configuration connects to the DuckDB layer, offering professional-grade dashboards and exploration tools.

## Key Features

- **Data Ownership:** All data is stored locally in open parquet formats. Your usage history is yours to keep, independent of platform changes.
- **Incremental Loading:** Intelligent state tracking ensures only new data is fetched, respecting API limits and optimizing performance.
- **Modern Stack:** Built on the efficient combination of DuckDB and Parquet (OLAP), capable of handling years of hourly data with minimal resource usage.
- **Ready-to-use Dashboards:** Comes with Superset setup to visualize trends, heatmaps, and load profiles out of the box.

## Use Cases & Automation

### Automated Daily Ingestion
Powerview is designed to run unattended. You can schedule the ingestion script to run daily (e.g., at 08:00 UTC when data is usually available) using standard system tools.

**Example Crontab:**
```bash
0 8 * * * cd /home/user/powerview && poetry run python -m powerview.src.main >> /var/log/powerview.log 2>&1
```

### Home Server Deployment
The stack is lightweight enough to run on a Raspberry Pi, Synology NAS, or any small Linux server. The Superset instance runs in Docker, and the data pipeline requires only a Python environment.

### Ad-hoc Data Science
Because the data is stored in standard DuckDB and Parquet formats, you aren't limited to the built-in tools. You can connect directly to `duckdb/analytics.duckdb` using:
- **Jupyter Notebooks** for advanced forecasting.
- **DBeaver** or **DataGrip** for SQL exploration.
- **R** or **Excel** for statistical analysis.

Use the navigation on the left to get started, review data storage, or configure Superset.
