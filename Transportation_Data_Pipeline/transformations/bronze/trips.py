from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

SOURCE_PATH = "/Volumes/transportation/bronze/data-store/trips/Full Load/"

@dp.table(
    name = "transportation.bronze.trips",
    comment="Streaming ingestion of raw orders data with Auto Loader",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed":"true",
        "delta.autoOptimize.optimizeWrite":"true",
        "delta.autoOptimize.autoCompact":"true",
    }
)

def orders_bronze():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes","true")
        .option("cloudFiles.schemaEvolutionMode","rescue")
        .option("cloudFiles.maxFilesPerTrigger",100)
        .load(SOURCE_PATH)
    )

    df = df.withColumnRenamed(
        "distance_travelled(km)","distance_travelled_km"
    )

    df = df.withColumn("file_name", col("_metadata.file_path"))\
        .withColumn("ingest_datetime", current_timestamp())

    return df