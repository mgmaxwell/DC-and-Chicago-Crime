# crime_funcs.py (this is basically the code from your notebook)

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql import functions as F, Window

def row_count(df: DataFrame) -> int:
    """Return total number of rows."""
    return df.count()

def crimes_by_hour(df: DataFrame) -> DataFrame:
    """Return total crimes per hour."""
    return (
        df.groupBy("hour")
          .agg(f.sum("crime_count").alias("crime_count"))
          .orderBy("hour")
    )

def crimes_by_day(df: DataFrame) -> DataFrame:
    """Return crimes by day."""
    return (
        df.groupBy("day_of_week")
          .agg(f.sum("crime_count").alias("crime_count"))
          .orderBy("day_of_week")
    )

def sum_crimes(df: DataFrame) -> DataFrame:
    """Return total number of crimes per city."""
    return df.groupBy("city").sum("crime_count")

def max_crimes(df: DataFrame) -> DataFrame:
    window = Window.partitionBy("city").orderBy(F.desc("crime_count"))
    return (
        df.withColumn("rank", F.rank().over(window))
          .filter("rank = 1")
          .select("city", "month", "crime_count")
    )

def min_crimes(df: DataFrame) -> DataFrame:
    window = Window.partitionBy("city").orderBy(F.asc("crime_count"))
    return (
        df.withColumn("rank", F.rank().over(window))
          .filter("rank = 1")
          .select("city", "month", "crime_count")
    )

def least_offense_committed(df: DataFrame) -> DataFrame:
    """Return the least committed offense per city."""
    w = Window.partitionBy("city").orderBy(F.asc("crime_count"))
    return (
        df.withColumn("rank", F.row_number().over(w))
          .filter(F.col("rank") == 1)
          .select("city", "offense_type", "crime_count")
          .orderBy("city")
    )

def max_crimes_violent(df: DataFrame) -> DataFrame:
    window = Window.partitionBy("city").orderBy(F.desc("crime_count"))
    return (
        df.withColumn("rank", F.rank().over(window))
          .filter("rank = 1")
          .select("city", "month", "crime_count", "crime_group")
    )

def sum_violent_crimes(df: DataFrame) -> DataFrame:
    """Return total crimes per city and crime_group."""
    return df.groupBy("city", "crime_group").sum("crime_count")
