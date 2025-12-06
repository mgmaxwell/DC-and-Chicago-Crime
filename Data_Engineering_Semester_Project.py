# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month
import requests

spark = SparkSession.builder.appName("CrimePipeline").getOrCreate()

# COMMAND ----------

dc_df = spark.read.csv("/Volumes/workspace/crime_data_project/dc_crime/DC_clean_Crime.csv", header=True, inferSchema=True)

dc_df.printSchema()
dc_df.write.mode("overwrite").saveAsTable(
    "workspace.crime_data_project.dc_crimes"
)

# COMMAND ----------

import os
import requests

# API endpoint from website in the csv format 
url = "https://data.cityofchicago.org/resource/t7ek-mgzi.csv"

# the token I created in my chicago data account 
headers = {"X-App-Token": "hlI1VlPCBzzfYPkBQcpEjXWzD"}  #

# API params: get up to 50k rows, ordered by date
params = {
    "$order": "date ASC",
    "$limit": 200000
}

#The path to my catalog, volume, and file on databricks
local_path = "/Volumes/workspace/crime_data_project/chicago_data/chicago_crime.csv"

# Ensuring directory exists (it does)
os.makedirs(os.path.dirname(local_path), exist_ok=True)

# Get data from API 
print("Getting Chicago crime data from API...")
response = requests.get(url, headers=headers, params=params, timeout=120)
response.raise_for_status()

# Write CSV directly to the Volume 
print(f" Writing CSV to {local_path} ...")
with open(local_path, "w", encoding="utf-8") as f:
    f.write(response.text)

# Read into Spark 
print(" Loading CSV into Spark DataFrame...")
df_chicago = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(local_path)
)

print(" Data successfully ingested from API and loaded into Spark!")
df_chicago.printSchema()
display(df_chicago.limit(20))

# Save Chicago clean table into Lakehouse
df_chicago.write.mode("overwrite").saveAsTable(
    "workspace.crime_data_project.chicago_crimes"
)


print("✅ Cleaned tables written: workspace.crime_data_project.chicago_crimes & dc_crimes")

# COMMAND ----------

# MAGIC %sql
# MAGIC --1) Monthly crime counts per city
# MAGIC CREATE OR REPLACE TABLE workspace.crime_data_project.crime_monthly AS
# MAGIC WITH chi AS (
# MAGIC   SELECT
# MAGIC     'Chicago' AS city,
# MAGIC     date_trunc('month', date) AS month_start
# MAGIC   FROM workspace.crime_data_project.chicago_crimes
# MAGIC ),
# MAGIC dc AS (
# MAGIC   SELECT
# MAGIC     'DC' AS city,
# MAGIC     date_trunc(
# MAGIC       'month',
# MAGIC       try_to_timestamp(REPORT_DAT, 'M/d/yyyy, h:mm:ss a')
# MAGIC     ) AS month_start
# MAGIC   FROM workspace.crime_data_project.dc_crimes
# MAGIC )
# MAGIC SELECT
# MAGIC   city,
# MAGIC   year(month_start)  AS year,
# MAGIC   month(month_start) AS month,
# MAGIC   COUNT(*)           AS crime_count
# MAGIC FROM (
# MAGIC   SELECT * FROM chi
# MAGIC   UNION ALL
# MAGIC   SELECT * FROM dc
# MAGIC )
# MAGIC WHERE month_start IS NOT NULL
# MAGIC GROUP BY city, year, month;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.crime_data_project.crime_monthly;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 2) Crime by offense type
# MAGIC CREATE OR REPLACE TABLE workspace.crime_data_project.crime_by_offense AS
# MAGIC SELECT
# MAGIC   'Chicago' AS city,
# MAGIC   primary_type AS offense_type,
# MAGIC   COUNT(*) AS crime_count
# MAGIC FROM workspace.crime_data_project.chicago_crimes
# MAGIC GROUP BY primary_type
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'DC' AS city,
# MAGIC   OFFENSE AS offense_type,
# MAGIC   COUNT(*) AS crime_count
# MAGIC FROM workspace.crime_data_project.dc_crimes
# MAGIC GROUP BY OFFENSE;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.crime_data_project.crime_by_offense;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3) Crime by offense type, normalized within each city
# MAGIC CREATE OR REPLACE TABLE workspace.crime_data_project.crime_by_offense_norm AS
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     'Chicago' AS city,
# MAGIC     primary_type AS offense_type,
# MAGIC     COUNT(*) AS crime_count
# MAGIC   FROM workspace.crime_data_project.chicago_crimes
# MAGIC   GROUP BY primary_type
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   SELECT
# MAGIC     'DC' AS city,
# MAGIC     OFFENSE AS offense_type,
# MAGIC     COUNT(*) AS crime_count
# MAGIC   FROM workspace.crime_data_project.dc_crimes
# MAGIC   GROUP BY OFFENSE
# MAGIC ),
# MAGIC totals AS (
# MAGIC   SELECT
# MAGIC     city,
# MAGIC     SUM(crime_count) AS total_crime
# MAGIC   FROM base
# MAGIC   GROUP BY city
# MAGIC )
# MAGIC SELECT
# MAGIC   b.city,
# MAGIC   b.offense_type,
# MAGIC   b.crime_count,
# MAGIC   t.total_crime,
# MAGIC   b.crime_count * 1.0 / t.total_crime AS offense_share
# MAGIC FROM base b
# MAGIC JOIN totals t
# MAGIC   ON b.city = t.city
# MAGIC ORDER BY crime_count, city; 
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.crime_data_project.crime_by_offense_norm;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4) Monthly comparison between cities
# MAGIC CREATE OR REPLACE TABLE workspace.crime_data_project.crime_monthly_compare AS
# MAGIC SELECT
# MAGIC   year, 
# MAGIC   month,
# MAGIC   COALESCE(MAX(CASE WHEN city = 'Chicago' THEN crime_count END), 0) AS chicago_crime_count,
# MAGIC   COALESCE(MAX(CASE WHEN city = 'DC' THEN crime_count END), 0)      AS dc_crime_count,
# MAGIC   COALESCE(MAX(CASE WHEN city = 'Chicago' THEN crime_count END), 0)
# MAGIC     - COALESCE(MAX(CASE WHEN city = 'DC' THEN crime_count END), 0)  AS diff_chi_minus_dc,
# MAGIC   CASE 
# MAGIC     WHEN COALESCE(MAX(CASE WHEN city = 'DC' THEN crime_count END), 0) > 0
# MAGIC       THEN COALESCE(MAX(CASE WHEN city = 'Chicago' THEN crime_count END), 0) * 1.0
# MAGIC            / COALESCE(MAX(CASE WHEN city = 'DC' THEN crime_count END), 0)
# MAGIC     ELSE NULL
# MAGIC   END AS chi_to_dc_ratio
# MAGIC FROM workspace.crime_data_project.crime_monthly
# MAGIC GROUP BY year, month
# MAGIC ORDER BY year, month;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.crime_data_project.crime_monthly_compare;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 5) Violent vs non-violent per month
# MAGIC CREATE OR REPLACE TABLE workspace.crime_data_project.crime_vp_monthly AS
# MAGIC WITH chi AS (
# MAGIC   SELECT
# MAGIC     'Chicago' AS city,
# MAGIC     date_trunc('month', date) AS month_start,
# MAGIC     CASE
# MAGIC       WHEN upper(primary_type) IN (
# MAGIC         'HOMICIDE',
# MAGIC         'BATTERY',
# MAGIC         'ASSAULT',
# MAGIC         'ROBBERY',
# MAGIC         'CRIM SEXUAL ASSAULT',
# MAGIC         'SEX OFFENSE'
# MAGIC       ) THEN 'Violent Crime'
# MAGIC       ELSE 'Non-violent Crime'
# MAGIC     END AS crime_group
# MAGIC   FROM workspace.crime_data_project.chicago_crimes
# MAGIC ),
# MAGIC dc AS (
# MAGIC   SELECT
# MAGIC     'DC' AS city,
# MAGIC     date_trunc(
# MAGIC       'month',
# MAGIC       coalesce(
# MAGIC       try_to_timestamp(REPORT_DAT, 'M/d/yyyy, h:mm:ss a')
# MAGIC       )
# MAGIC     ) AS month_start,
# MAGIC     CASE
# MAGIC       WHEN upper(OFFENSEGROUP) LIKE 'VIOLENT%' THEN 'Violent Crime'
# MAGIC       WHEN upper(OFFENSEGROUP) LIKE 'PROPERTY%' THEN 'Non-violent Crime'
# MAGIC       ELSE 'Non-violent Crime'
# MAGIC     END AS crime_group
# MAGIC   FROM workspace.crime_data_project.dc_crimes
# MAGIC )
# MAGIC SELECT
# MAGIC   city,
# MAGIC   year(month_start)  AS year,
# MAGIC   month(month_start) AS month,
# MAGIC   crime_group,
# MAGIC   COUNT(*)           AS crime_count
# MAGIC FROM (
# MAGIC   SELECT * FROM chi
# MAGIC   UNION ALL
# MAGIC   SELECT * FROM dc
# MAGIC )
# MAGIC WHERE month_start IS NOT NULL
# MAGIC GROUP BY city, year, month, crime_group
# MAGIC ORDER BY year, month; 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.crime_data_project.crime_vp_monthly;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 6) Population-adjusted monthly crime rates
# MAGIC CREATE OR REPLACE TABLE workspace.crime_data_project.crime_monthly_pop_adjusted AS
# MAGIC SELECT
# MAGIC   year,
# MAGIC   month,
# MAGIC   chicago_crime_count,
# MAGIC   dc_crime_count,
# MAGIC   (chicago_crime_count * 100000.0 / 2700000) AS chi_per_100k,
# MAGIC   (dc_crime_count       * 100000.0 / 702000 ) AS dc_per_100k,
# MAGIC   chicago_crime_count - dc_crime_count AS diff_raw,
# MAGIC   (chicago_crime_count * 100000.0 / 2700000)
# MAGIC     - (dc_crime_count * 100000.0 / 702000) AS diff_per_100k,
# MAGIC   CASE
# MAGIC     WHEN dc_crime_count > 0 THEN 
# MAGIC       (chicago_crime_count * 1.0 / 2700000) /
# MAGIC       (dc_crime_count * 1.0 / 702000)
# MAGIC     ELSE NULL
# MAGIC   END AS chi_to_dc_rate_ratio
# MAGIC FROM workspace.crime_data_project.crime_monthly_compare;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.crime_data_project.crime_monthly_pop_adjusted;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 7) Crime by hour of day and day of week
# MAGIC CREATE OR REPLACE TABLE workspace.crime_data_project.crime_hourly AS
# MAGIC WITH chi_raw AS (
# MAGIC   SELECT
# MAGIC     'Chicago' AS city,
# MAGIC     coalesce(
# MAGIC       try_to_timestamp(Date),
# MAGIC       try_to_timestamp(Date, 'M/d/yyyy h:mm:ss a'),
# MAGIC       try_to_timestamp(Date, 'M/d/yyyy, h:mm:ss a')
# MAGIC     ) AS ts
# MAGIC   FROM workspace.crime_data_project.chicago_crimes
# MAGIC ),
# MAGIC dc_raw AS (
# MAGIC   SELECT
# MAGIC     'DC' AS city,
# MAGIC     coalesce(
# MAGIC       try_to_timestamp(REPORT_DAT),
# MAGIC       try_to_timestamp(REPORT_DAT, 'M/d/yyyy h:mm:ss a'),
# MAGIC       try_to_timestamp(REPORT_DAT, 'M/d/yyyy, h:mm:ss a')
# MAGIC     ) AS ts
# MAGIC   FROM workspace.crime_data_project.dc_crimes
# MAGIC ),
# MAGIC chi AS (
# MAGIC   SELECT
# MAGIC     city,
# MAGIC     date_format(ts, 'E') AS day_of_week,
# MAGIC     hour(ts)             AS hour
# MAGIC   FROM chi_raw
# MAGIC   WHERE ts IS NOT NULL
# MAGIC ),
# MAGIC dc AS (
# MAGIC   SELECT
# MAGIC     city,
# MAGIC     date_format(ts, 'E') AS day_of_week,
# MAGIC     hour(ts)             AS hour
# MAGIC   FROM dc_raw
# MAGIC   WHERE ts IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   city,
# MAGIC   day_of_week,
# MAGIC   hour,
# MAGIC   COUNT(*) AS crime_count
# MAGIC FROM (
# MAGIC   SELECT * FROM chi
# MAGIC   UNION ALL
# MAGIC   SELECT * FROM dc
# MAGIC )
# MAGIC GROUP BY city, day_of_week, hour 
# MAGIC ORDER BY day_of_week; 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.crime_data_project.crime_hourly;

# COMMAND ----------

# MAGIC %pip install prometheus_client

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

#This is just a demo on how we would use Prometheus in our pipeline if we had a prometheus running on an EC2 Instance. So this is not actually running and the output will be exception print. But if we had a prometheus running on an EC2 instance, we would be able to push the metrics to the prometheus server and then we can query the metrics from the prometheus server. 
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

dc_count = dc_df.count()
chi_count = df_chicago.count()

registry = CollectorRegistry()

row_gauge = Gauge(
    'crime_ingested_rows',
    'number of rows ingested',
    ['city'],
    registry=registry
)
row_gauge.labels(city='DC').set(dc_count)
row_gauge.labels(city='Chicago').set(chi_count)

try:
    push_to_gateway("localhost:9091", job='crime_pipeline', registry=registry)
    print(f"Pushed metrics to localhost:9091: DC={dc_count}, Chicago={chi_count}")
except Exception as e:
    print(f"Could not push metrics to localhost:9091: {e}")
