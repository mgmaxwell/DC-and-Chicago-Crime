# Databricks notebook source
from pyspark.sql import DataFrame 
from pyspark.sql import functions as f 

# COMMAND ----------

crime_time = spark.table("workspace.crime_data_project.crime_hourly")

# COMMAND ----------

def row_count(df: DataFrame) -> int:
    """Return total number of rows."""
    return df.count()
total = row_count(crime_time)
print(total)

# COMMAND ----------

def crimes_by_hour(df: DataFrame) -> DataFrame:
    """Return total crimes per hour."""
    return (
        df.groupBy("hour")
          .agg(f.sum("crime_count").alias("crime_count"))
          .orderBy("hour")
    )

display(crimes_by_hour(crime_time))

# COMMAND ----------

def crimes_by_day(df: DataFrame) -> DataFrame:
    """Return crimes by day."""
    return (
        df.groupBy("day_of_week")
        .agg(f.sum("crime_count").alias("crime_count"))
        .orderBy("day_of_week")
    )

display(crimes_by_day(crime_time))

# COMMAND ----------

crimes_per_month = spark.table("workspace.crime_data_project.crime_monthly")


# COMMAND ----------

def sum_crimes (df: DataFrame) -> DataFrame:
    """Return total number of crimes."""
    return df.groupBy("city").sum("crime_count")
sum_crimes(crimes_per_month)
display (sum_crimes(crimes_per_month))

# COMMAND ----------

from pyspark.sql import functions as F, Window

def max_crimes(df):
    window = Window.partitionBy("city").orderBy(F.desc("crime_count"))

    return (
        df.withColumn("rank", F.rank().over(window))
          .filter("rank = 1")       # keep only highest month per city
          .select("city", "month", "crime_count")
    )
display(max_crimes(crimes_per_month))

# COMMAND ----------

from pyspark.sql import functions as F, Window

def min_crimes(df):
    window = Window.partitionBy("city").orderBy(F.asc("crime_count"))

    return (
        df.withColumn("rank", F.rank().over(window))
          .filter("rank = 1")       # keep only lowest month per city
          .select("city", "month", "crime_count")
    )
display(min_crimes(crimes_per_month))

# COMMAND ----------

crime_offense = spark.table("workspace.crime_data_project.crime_by_offense")

# COMMAND ----------

from pyspark.sql import functions as F, Window
from pyspark.sql import DataFrame

def most_offense_committed(df: DataFrame) -> DataFrame:
    """Return the most committed offense for each city."""
    w = Window.partitionBy("city").orderBy(F.desc("crime_count"))

    return (
        df.withColumn("rank", F.row_number().over(w))  # rank offenses within each city
          .filter(F.col("rank") == 1)                  # keep only the top offense per city
          .select("city", "offense_type", "crime_count")
          .orderBy("city")
    )
display(most_offense_committed(crime_offense))



# COMMAND ----------

def least_offense_committed(df: DataFrame) -> DataFrame:
    """Return the least committed offense."""
    w = Window.partitionBy("city").orderBy(F.asc("crime_count"))

    return (
        df.withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") == 1)
        .select("city", "offense_type", "crime_count")
        .orderBy("city")
    )

display(least_offense_committed(crime_offense))

# COMMAND ----------

violent_vs_nonviolent = spark.table("workspace.crime_data_project.crime_vp_monthly")

# COMMAND ----------

def max_crimes_violent(df):
    window = Window.partitionBy("city").orderBy(F.desc("crime_count"))

    return (
        df.withColumn("rank", F.rank().over(window))
          .filter("rank = 1")       # keep only highest month per city
          .select("city", "month", "crime_count", "crime_group")
    )
display(max_crimes_violent(violent_vs_nonviolent))


# COMMAND ----------

def sum_violent_crimes (df: DataFrame) -> DataFrame:
    """Return total number of crimes."""
    return df.groupBy("city", "crime_group").sum("crime_count")
sum_violent_crimes(violent_vs_nonviolent)
display(sum_violent_crimes(violent_vs_nonviolent))
