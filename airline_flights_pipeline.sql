-- Airline Flight Operations Analytics
-- Lakeflow Spark Declarative Pipeline (SQL)
-- Default catalog: flights   Default schema: ops
-- Source: /Volumes/flights/ops/raw_files/airline_flights_raw.csv

-- =========================
-- BRONZE: raw data as-is
-- =========================
CREATE OR REFRESH STREAMING TABLE bronze_airline_flights AS
SELECT *,
       _metadata.file_name AS source_file,
       current_timestamp() AS loaded_at
FROM STREAM read_files('/Volumes/flights/ops/raw_files/',
                       format => 'csv', header => true);

-- =========================
-- SILVER: clean + rejected
-- =========================
CREATE OR REFRESH MATERIALIZED VIEW silver_flights_clean (
  CONSTRAINT valid_passengers EXPECT (passengers BETWEEN 0 AND seat_capacity) ON VIOLATION DROP ROW,
  CONSTRAINT valid_fare       EXPECT (avg_fare >= 0)                          ON VIOLATION DROP ROW
) AS
WITH cleaned AS (
  SELECT DISTINCT
    flight_id, flight_number,
    COALESCE(INITCAP(TRIM(airline)), 'Unknown')       AS airline,
    aircraft_id, aircraft_type, flight_date, day_of_week,
    INITCAP(TRIM(origin_city))                        AS origin_city,
    INITCAP(TRIM(destination_city))                   AS destination_city,
    origin_region, distance_km,
    scheduled_departure, actual_departure,
    COALESCE(delay_minutes,
             TIMESTAMPDIFF(MINUTE, scheduled_departure, actual_departure)) AS delay_minutes,
    COALESCE(INITCAP(TRIM(delay_reason)), 'Unknown')  AS delay_reason,
    seat_capacity, passengers, avg_fare,
    COALESCE(ticket_revenue, passengers * avg_fare)   AS ticket_revenue,
    fuel_cost
  FROM bronze_airline_flights
)
SELECT *,
  CASE WHEN delay_minutes <= 15 THEN 'On Time'
       WHEN delay_minutes <= 60 THEN 'Minor Delay'
       ELSE 'Major Delay' END AS delay_category
FROM cleaned;

CREATE OR REFRESH MATERIALIZED VIEW silver_flights_rejected AS
SELECT *,
  CASE WHEN passengers > seat_capacity THEN 'Passengers more than seats'
       ELSE 'Average fare is negative' END AS reject_reason
FROM bronze_airline_flights
WHERE passengers > seat_capacity OR passengers < 0 OR avg_fare < 0;

-- =========================
-- GOLD: business summaries
-- =========================
CREATE OR REFRESH MATERIALIZED VIEW gold_monthly_flights AS
SELECT DATE_TRUNC('MONTH', flight_date) AS flight_month,
       COUNT(*)                         AS flights,
       SUM(passengers)                  AS passengers,
       SUM(seat_capacity)               AS seats,
       ROUND(SUM(passengers) * 100.0 / SUM(seat_capacity), 1) AS load_factor_pct,
       SUM(ticket_revenue)              AS ticket_revenue,
       SUM(fuel_cost)                   AS fuel_cost,
       SUM(CASE WHEN delay_category <> 'On Time' THEN 1 ELSE 0 END) AS delayed_flights
FROM silver_flights_clean
GROUP BY DATE_TRUNC('MONTH', flight_date);

CREATE OR REFRESH MATERIALIZED VIEW gold_airline_summary AS
SELECT airline,
       COUNT(*)            AS flights,
       SUM(passengers)     AS passengers,
       SUM(ticket_revenue) AS ticket_revenue,
       ROUND(SUM(passengers) * 100.0 / SUM(seat_capacity), 1) AS load_factor_pct,
       ROUND(SUM(CASE WHEN delay_category = 'On Time' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS on_time_pct,
       ROUND(AVG(delay_minutes), 1) AS avg_delay_minutes
FROM silver_flights_clean
GROUP BY airline;

CREATE OR REFRESH MATERIALIZED VIEW gold_route_summary AS
SELECT origin_city, destination_city,
       COUNT(*)            AS flights,
       SUM(passengers)     AS passengers,
       SUM(ticket_revenue) AS ticket_revenue,
       ROUND(SUM(passengers) * 100.0 / SUM(seat_capacity), 1) AS load_factor_pct
FROM silver_flights_clean
GROUP BY origin_city, destination_city;

CREATE OR REFRESH MATERIALIZED VIEW gold_delay_summary AS
SELECT delay_category, delay_reason,
       COUNT(*) AS flights,
       ROUND(AVG(delay_minutes), 1) AS avg_delay_minutes
FROM silver_flights_clean
GROUP BY delay_category, delay_reason;

-- =========================
-- GOLD: data quality + reconciliation
-- =========================
CREATE OR REFRESH MATERIALIZED VIEW gold_data_quality AS
WITH counts AS (
  SELECT COUNT(*) AS bronze_rows,
         COUNT(*) - COUNT(DISTINCT flight_id) AS duplicate_rows,
         SUM(CASE WHEN passengers > seat_capacity THEN 1 ELSE 0 END) AS overbooked_rows,
         SUM(CASE WHEN avg_fare < 0 THEN 1 ELSE 0 END)               AS negative_fare_rows,
         SUM(CASE WHEN airline IS NULL THEN 1 ELSE 0 END)            AS missing_airline_rows,
         SUM(CASE WHEN delay_minutes IS NULL THEN 1 ELSE 0 END)      AS missing_delay_rows,
         SUM(CASE WHEN ticket_revenue IS NULL THEN 1 ELSE 0 END)     AS missing_revenue_rows,
         SUM(CASE WHEN delay_reason IS NULL THEN 1 ELSE 0 END)       AS missing_reason_rows,
         SUM(CASE WHEN aircraft_id IS NULL THEN 1 ELSE 0 END)        AS missing_aircraft_rows
  FROM bronze_airline_flights
),
totals AS (
  SELECT c.*,
         (SELECT COUNT(*) FROM silver_flights_clean)    AS clean_rows,
         (SELECT COUNT(*) FROM silver_flights_rejected) AS rejected_rows
  FROM counts c
)
SELECT *,
       CASE WHEN bronze_rows = clean_rows + rejected_rows + duplicate_rows
            THEN 'Yes' ELSE 'No' END AS reconciliation_passed
FROM totals;
