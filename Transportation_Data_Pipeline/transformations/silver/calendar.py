from pyspark import pipelines as dp
from pyspark.sql.functions import col, lit, year, month, quarter, dayofmonth, date_format, dayofweek, weekofyear, when,concat,dayofyear,current_timestamp

start_date = spark.conf.get("start_date")
end_date = spark.conf.get("end_date")

@dp.materialized_view(
    name="transportation.silver.calendar",
    comment="Calendar dimension with comprehensive date attributes and Indian holidays (2025)",
    table_properties={
        "quality": "transportation.silver.calendar",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    }
)

def calendar():
    df = spark.sql(f"""
                   SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as date
                   """)
    
    df= df.withColumn("date_key",date_format(col("date"), "yyyyMMdd").cast("int"))

    df = (df.withColumn("year",year(col("date")))
        .withColumn("month",month(col("date")))
        .withColumn("quarter",quarter(col("date")))
    )

    df = (
        df.withColumn("day_of_month",dayofmonth(col("date")))
        .withColumn("day_of_week",date_format(col("date"),"EEEE"))
        .withColumn("day_of_week_abbr",date_format(col("date"),"EEE"))
        .withColumn("day_of_week_num",dayofweek(col("date")))
    )

    df = (
        df.withColumn("month_name",date_format(col("date"),"MMMM"))
        .withColumn("month_year",concat(date_format(col("date"),"MMMM"), lit(" "), col("year")))
        .withColumn("quarter_year",concat(lit("Q"),col("quarter"),lit(" "), col("year")))
    )

    df = df.withColumn(
        "week_of_year", weekofyear(col("date"))
    ).withColumn("day_of_year", dayofyear(col("date")))

    df = df.withColumn(
        "is_weekend",
        when(col("day_of_week_num").isin([1, 7]), True).otherwise(False)).withColumn(
        "is_weekday", when(col("day_of_week_num").isin([1, 7]), False).otherwise(True),
    )

    df = df.withColumn(
        "holiday_name",
        when(
            (col("month") == 1) & (col("day_of_month") == 26), lit("Republic Day")
        )
        .when(
            (col("month") == 8) & (col("day_of_month") == 15),
            lit("Independence Day"),
        )
        .when(
            (col("month") == 10) & (col("day_of_month") == 2),
            lit("Gandhi Jayanti"),
        )
        .otherwise(None),
    ).withColumn(
        "is_holiday", when(col("holiday_name").isNotNull(), True).otherwise(False)
    )

    df = df.withColumn(
        "silver_processed_timestamp", current_timestamp()
    )
    
    df_silver = df.select(
        "date",
        "date_key",
        "year",
        "month",
        "day_of_month",
        "day_of_week",
        "day_of_week_abbr",
        "month_name",
        "month_year",
        "quarter",
        "quarter_year",
        "week_of_year",
        "day_of_year",
        "is_weekday",
        "is_weekend",
        "is_holiday",
        "holiday_name",
        "silver_processed_timestamp"
    )


    return df