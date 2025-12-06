from crime_func import row_count
def test_row_count(spark):
    data = [
        ("Chicago", 1, 10),
        ("DC",      2, 5),
    ]
    df = spark.createDataFrame(data, ["city", "hour", "crime_count"])

    assert row_count(df) == 2

    
from crime_func import crimes_by_hour

def test_crimes_by_hour(spark):
    data = [
        ("Chicago", 1, 10),
        ("Chicago", 1, 5),
        ("DC",      2, 7),
    ]
    df = spark.createDataFrame(data, ["city", "hour", "crime_count"])

    result = crimes_by_hour(df).orderBy("hour").collect()

    # hour 1: 10 + 5 = 15, hour 2: 7
    assert result[0]["hour"] == 1
    assert result[0]["crime_count"] == 15
    assert result[1]["hour"] == 2
    assert result[1]["crime_count"] == 7

    from crime_func import sum_violent_crimes

def test_sum_violent_crimes(spark):
    data = [
        ("Chicago", "violent", 10),
        ("Chicago", "violent", 5),
        ("Chicago", "nonviolent", 20),
        ("DC", "violent", 7),
    ]
    df = spark.createDataFrame(data, ["city", "crime_group", "crime_count"])

    result_df = sum_violent_crimes(df)
    rows = {(r["city"], r["crime_group"]): r["sum(crime_count)"]
            for r in result_df.collect()}

    assert rows[("Chicago", "violent")] == 15
    assert rows[("Chicago", "nonviolent")] == 20
    assert rows[("DC", "violent")] == 7

