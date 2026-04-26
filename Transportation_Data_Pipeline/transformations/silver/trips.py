from pyspark import pipelines as dp
from pyspark.sql.functions import col, lower, current_timestamp

@dp.view(
    name= "trips_silver_staging",
    comment = "Transformed trips data ready for CDC  upsert"
)
@dp.expect("valid_date","year(business_date)>=2020")
@dp.expect("valid_driver_rating","driver_rating BETWEEN 1 AND 10")
@dp.expect("valid_passenger_rating","passenger_rating BETWEEN 1 AND 10")

def trips_silver_staging():
    df_bronze = spark.readStream.table("transportation.bronze.trips")
    df_silver = df_bronze.withColumn("passenger_type",lower("passenger_type")).select(
        col("trip_id").alias("id"),
        col("date").alias("business_date"),
        col("city_id").alias("city_id"),
        col("passenger_type").alias("passenger_catergory"),
        col("distance_travelled_km").alias("distance_kms"),
        col("fare_amount").alias("sales_amt"),
        col("passenger_rating").alias("passenger_rating"),
        col("driver_rating").alias("driver_rating"),
        col("ingest_datetime").alias("bronze_ingest_timestamp"),
    )

    df_silver = df_silver.withColumn("silver_processed_timestamp",current_timestamp())
    return df_silver

dp.create_streaming_table(
    name = "transportation.silver.trips",
    comment = "Cleaned and validated orders with CDC upsert capability",
    table_properties= {
        "quality":"silver",
        "layer":"silver",
        "delta.enableChangeDataFeed":"true",
        "delta.autoOptimize.optimizeWrite":"true",
        "delta.autoOptimize.autoCompact":"true"
    }
)

dp.create_auto_cdc_flow(
    target = "transportation.silver.trips",
    source = "trips_silver_staging",
    keys = ["id"],
    sequence_by =col("silver_processed_timestamp"),
    stored_as_scd_type = 1,
    except_column_list = []
)