
-- Creating Analysis Schema

CREATE SCHEMA IF NOT EXISTS workspace.crime_analysis;



-- Crime Coordinates (Monthly) 
-- For the Chicago location maps (all types, theft-only,
-- battery/assault, clustering)

CREATE OR REPLACE TABLE crime_lakehouse.analysis.crime_coordinates_monthly AS
WITH chi AS (
  SELECT
    'Chicago' AS city,
    coalesce(
      try_to_timestamp(`Date`),
      try_to_timestamp(`Date`, 'M/d/yyyy h:mm:ss a'),
      try_to_timestamp(`Date`, 'M/d/yyyy, h:mm:ss a')
    ) AS ts,
    CAST(Longitude AS DOUBLE) AS longitude,
    CAST(Latitude  AS DOUBLE) AS latitude,
    `Primary Type` AS offense_type
  FROM workspace.crime_data_project.chicago_crimes
  WHERE Latitude IS NOT NULL AND Longitude IS NOT NULL
),
dc AS (
  SELECT
    'DC' AS city,
    coalesce(
      try_to_timestamp(`REPORT_DAT`),
      try_to_timestamp(`REPORT_DAT`, 'M/d/yyyy h:mm:ss a'),
      try_to_timestamp(`REPORT_DAT`, 'M/d/yyyy, h:mm:ss a')
    ) AS ts,
    CAST(LONGITUDE AS DOUBLE) AS longitude,
    CAST(LATITUDE  AS DOUBLE) AS latitude,
    OFFENSE AS offense_type
  FROM workspace.crime_data_project.dc_crimes
  WHERE LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
)
SELECT
  city,
  year(ts)  AS year,
  month(ts) AS month,
  longitude,
  latitude,
  offense_type
FROM (
  SELECT * FROM chi
  UNION ALL
  SELECT * FROM dc
)
WHERE ts IS NOT NULL;



-- Crime Hourly Table
-- For DC heatmap
CREATE OR REPLACE TABLE crime_lakehouse.analysis.crime_hourly AS
WITH chi_raw AS (
  SELECT
    'Chicago' AS city,
    coalesce(
      try_to_timestamp(`Date`),
      try_to_timestamp(`Date`, 'M/d/yyyy h:mm:ss a'),
      try_to_timestamp(`Date`, 'M/d/yyyy, h:mm:ss a')
    ) AS ts
  FROM workspace.crime_data_project.chicago_crimes
),
dc_raw AS (
  SELECT
    'DC' AS city,
    coalesce(
      try_to_timestamp(`REPORT_DAT`),
      try_to_timestamp(`REPORT_DAT`, 'M/d/yyyy h:mm:ss a'),
      try_to_timestamp(`REPORT_DAT`, 'M/d/yyyy, h:mm:ss a')
    ) AS ts
  FROM workspace.crime_data_project.dc_crimes
),
chi AS (
  SELECT
    city,
    date_format(ts, 'E') AS day_of_week,
    hour(ts)            AS hour
  FROM chi_raw
  WHERE ts IS NOT NULL
),
dc AS (
  SELECT
    city,
    date_format(ts, 'E') AS day_of_week,
    hour(ts)            AS hour
  FROM dc_raw
  WHERE ts IS NOT NULL
)
SELECT
  city,
  day_of_week,
  hour,
  COUNT(*) AS crime_count
FROM (
  SELECT * FROM chi
  UNION ALL
  SELECT * FROM dc
)
GROUP BY city, day_of_week, hour;



-- Crime Monthly (Raw Counts Per City)
-- For the population-adjusted line chart

CREATE OR REPLACE TABLE crime_lakehouse.analysis.crime_monthly AS
WITH chi AS (
  SELECT
    'Chicago' AS city,
    date_trunc(
      'month',
      coalesce(
        try_to_timestamp(`Date`),
        try_to_timestamp(`Date`, 'M/d/yyyy h:mm:ss a'),
        try_to_timestamp(`Date`, 'M/d/yyyy, h:mm:ss a')
      )
    ) AS month_start
  FROM workspace.crime_data_project.chicago_crimes
),
dc AS (
  SELECT
    'DC' AS city,
    date_trunc(
      'month',
      coalesce(
        try_to_timestamp(`REPORT_DAT`),
        try_to_timestamp(`REPORT_DAT`, 'M/d/yyyy h:mm:ss a'),
        try_to_timestamp(`REPORT_DAT`, 'M/d/yyyy, h:mm:ss a')
      )
    ) AS month_start
  FROM workspace.crime_data_project.dc_crimes
)
SELECT
  city,
  year(month_start)  AS year,
  month(month_start) AS month,
  COUNT(*)           AS crime_count
FROM (
  SELECT * FROM chi
  UNION ALL
  SELECT * FROM dc
)
WHERE month_start IS NOT NULL
GROUP BY city, year, month;



-- Compare Chicago vs DC Monthly Totals
-- for the relative comparison

CREATE OR REPLACE TABLE crime_lakehouse.analysis.crime_monthly_compare AS
SELECT
  year,
  month,
  COALESCE(MAX(CASE WHEN city = 'Chicago' THEN crime_count END), 0) AS chicago_crime_count,
  COALESCE(MAX(CASE WHEN city = 'DC'      THEN crime_count END), 0) AS dc_crime_count,

  -- Raw difference
  COALESCE(MAX(CASE WHEN city = 'Chicago' THEN crime_count END), 0)
    - COALESCE(MAX(CASE WHEN city = 'DC'  THEN crime_count END), 0) AS diff_chi_minus_dc,

  -- Ratio (Chicago / DC)
  CASE 
    WHEN COALESCE(MAX(CASE WHEN city = 'DC' THEN crime_count END), 0) > 0
      THEN COALESCE(MAX(CASE WHEN city = 'Chicago' THEN crime_count END), 0) * 1.0
           / COALESCE(MAX(CASE WHEN city = 'DC'      THEN crime_count END), 0)
    ELSE NULL
  END AS chi_to_dc_ratio
FROM crime_lakehouse.analysis.crime_monthly
GROUP BY year, month;



-- Population-Adjusted Monthly Rates
-- for the final population-adjusted line chart

CREATE OR REPLACE TABLE crime_lakehouse.analysis.crime_monthly_pop_adjusted AS
SELECT
  year,
  month,
  chicago_crime_count,
  dc_crime_count,

  (chicago_crime_count * 100000.0 / 2700000) AS chi_per_100k,
  (dc_crime_count       * 100000.0 / 702000 ) AS dc_per_100k,

  chicago_crime_count - dc_crime_count AS diff_raw,

  (chicago_crime_count * 100000.0 / 2700000)
    - (dc_crime_count * 100000.0 / 702000) AS diff_per_100k,

  CASE
    WHEN dc_crime_count > 0 THEN 
      (chicago_crime_count * 1.0 / 2700000) /
      (dc_crime_count      * 1.0 / 702000)
    ELSE NULL
  END AS chi_to_dc_rate_ratio

FROM crime_lakehouse.analysis.crime_monthly_compare;



-- Offense Counts (Chicago vs DC)
-- For the crime category bar charts

CREATE OR REPLACE TABLE crime_lakehouse.analysis.chi_offense_counts AS
SELECT
  `Primary Type` AS offense_type,
  COUNT(*)       AS crime_count
FROM workspace.crime_data_project.chicago_crimes
GROUP BY `Primary Type`;


CREATE OR REPLACE TABLE crime_lakehouse.analysis.dc_offense_counts AS
SELECT
  OFFENSE AS offense_type,
  COUNT(*) AS crime_count
FROM workspace.crime_data_project.dc_crimes
GROUP BY OFFENSE;
