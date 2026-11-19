"""Transformations Spark, séparées du job pour être testées sans Kafka ni S3."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

STATUS_SCHEMA = StructType([
    StructField("schema", IntegerType()),
    StructField("station_id", StringType()),
    StructField("station_code", StringType()),
    StructField("bikes", IntegerType()),
    StructField("mechanical", IntegerType()),
    StructField("ebike", IntegerType()),
    StructField("docks", IntegerType()),
    StructField("is_installed", BooleanType()),
    StructField("is_renting", BooleanType()),
    StructField("is_returning", BooleanType()),
    StructField("last_reported", LongType()),
    StructField("polled_at", LongType()),
])

INFO_SCHEMA = StructType([
    StructField("schema", IntegerType()),
    StructField("station_id", StringType()),
    StructField("station_code", StringType()),
    StructField("name", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("capacity", IntegerType()),
    StructField("polled_at", LongType()),
])


def bronze(kafka_df: DataFrame) -> DataFrame:
    """Le message tel qu'il est arrivé, avec sa position dans Kafka : rien n'est perdu."""
    return kafka_df.select(
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        "topic", "partition", "offset",
        F.col("timestamp").alias("kafka_ts"),
        F.to_date("timestamp").alias("date"),
    )


def parse_status(df: DataFrame) -> DataFrame:
    m = F.from_json(F.col("value").cast("string"), STATUS_SCHEMA).alias("m")
    return (df.select(m).select("m.*")
            .where((F.col("schema") == 1) & F.col("station_id").isNotNull())
            .withColumn("reported_at", F.timestamp_seconds("last_reported"))
            .withColumn("polled_at", F.timestamp_seconds("polled_at"))
            .withColumn("date", F.to_date("reported_at"))
            .drop("schema"))


def parse_info(df: DataFrame) -> DataFrame:
    m = F.from_json(F.col("value").cast("string"), INFO_SCHEMA).alias("m")
    return (df.select(m).select("m.*")
            .where((F.col("schema") == 1) & F.col("station_id").isNotNull())
            .withColumn("polled_at", F.timestamp_seconds("polled_at"))
            .drop("schema"))


def dedup_status(df: DataFrame) -> DataFrame:
    """Un relevé = (station, horodatage du relevé). Kafka peut livrer deux fois."""
    return df.dropDuplicates(["station_id", "last_reported"])


def latest_info(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("station_id").orderBy(F.col("polled_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
