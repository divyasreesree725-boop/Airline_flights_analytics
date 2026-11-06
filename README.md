# Airline Flight Operations Analytics

A Databricks Medallion pipeline (Bronze, Silver, Gold) built with Lakeflow Spark Declarative Pipelines in SQL, plus a 3-page AI/BI dashboard. It turns a raw airline flights CSV (1,365 rows) into trusted summary tables and proves that no row was lost.

**Tools:** Databricks, Unity Catalog, Lakeflow Spark Declarative Pipelines (SQL), Databricks AI/BI

## Dashboard

### Overview
![Overview](Dashboard_Overview.png)

### Airlines, Routes and Delays
![Airlines, Routes and Delays](Dashboard_airlines_routes_delays.png)

### Data Quality
![Data Quality](dashboard_data_quality.png)

## Pipeline

![Pipeline graph](Pipeline_Graph.png)

Bronze keeps the raw data, Silver cleans it and quarantines bad rows, and Gold builds the summary tables.

## Data quality and reconciliation

![Data quality result](Data_Quality_Result.png)

1,365 Bronze rows = 1,321 clean + 19 rejected + 25 duplicates removed.

## Gold table example

![Airline summary](Gold_Airline_summary.png)

## Cleaning rules

| Problem | Rule | Why |
|---|---|---|
| 25 duplicate flights | SELECT DISTINCT | flight_id must be unique |
| Extra spaces, mixed case | INITCAP(TRIM(col)) | Group names consistently |
| Blank airline and delay reason | COALESCE to 'Unknown' | Keep the flight, show the gap |
| Blank delay minutes | TIMESTAMPDIFF of the two departure times | Value can be calculated |
| Blank ticket revenue | passengers x avg_fare | Formula defines revenue |
| Passengers above seats, negative fare | Moved to the rejected table with a reason | Impossible values |
| Negative delay | Kept, counted as On Time | The flight left early |

## How to run

1. Create catalog `flights`, schema `ops` and volume `raw_files`.
2. Upload `airline_flights_raw.csv` to `/Volumes/flights/ops/raw_files/`.
3. Create an ETL pipeline (default catalog `flights`, schema `ops`) and add `airline_flights_pipeline.sql`.
4. Click Run pipeline, then refresh the dashboard.

## Files

- `airline_flights_pipeline.sql`: full pipeline code
- Screenshots: pipeline graph, data quality result, Gold table and the three dashboard pages
