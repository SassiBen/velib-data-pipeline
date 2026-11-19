"""Kafka -> bronze -> silver, en continu, sur un lac Delta en stockage S3 (Garage en local).

- bronze : chaque message, brut, partitionné par jour. C'est l'assurance de
  pouvoir tout recalculer si une règle de la couche silver change.
- silver : relevés typés et dédoublonnés (MERGE idempotent), et un référentiel
  des stations tenu à jour (upsert).
Ce job est le seul écrivain des tables Delta : compaction et nettoyage sont
faits ici, pas par un second processus qui pourrait entrer en conflit avec lui.
"""

import os

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from transforms import bronze, dedup_status, latest_info, parse_info, parse_status

LAKE = os.getenv("LAKE_URI", "s3a://lake")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TRIGGER = os.getenv("TRIGGER", "1 minute")
OPTIMIZE_EVERY = int(os.getenv("OPTIMIZE_EVERY", "60"))      # micro-lots, soit ~1 h
VACUUM_EVERY = int(os.getenv("VACUUM_EVERY", "1440"))        # ~1 jour


def session() -> SparkSession:
    return (
        SparkSession.builder.appName("velib-stream")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", os.environ["S3_ENDPOINT"])
        .config("spark.hadoop.fs.s3a.access.key", os.environ["S3_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["S3_SECRET_KEY"])
        .config("spark.hadoop.fs.s3a.endpoint.region", os.getenv("S3_REGION", "garage"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Garage (comme d'autres stockages S3 hors AWS) répond NoSuchKey quand on
        # supprime en lot une clé absente ; S3A, qui nettoie ses marqueurs de
        # répertoire après chaque écriture, réessayait alors indéfiniment.
        .config("spark.hadoop.fs.s3a.directory.marker.retention", "keep")
        .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_topic(spark: SparkSession, topic: str):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load())


def maintain(spark: SparkSession, path: str, batch_id: int) -> None:
    if batch_id and batch_id % OPTIMIZE_EVERY == 0:
        DeltaTable.forPath(spark, path).optimize().executeCompaction()
    if batch_id and batch_id % VACUUM_EVERY == 0:
        DeltaTable.forPath(spark, path).vacuum(168)


def main() -> None:
    spark = session()
    status_path = f"{LAKE}/silver/station_status"
    stations_path = f"{LAKE}/silver/stations"

    def upsert_status(batch, batch_id):
        rows = dedup_status(parse_status(batch))
        if not DeltaTable.isDeltaTable(spark, status_path):
            rows.write.format("delta").partitionBy("date").save(status_path)
            return
        (DeltaTable.forPath(spark, status_path).alias("t")
         .merge(rows.alias("s"), "t.date = s.date AND t.station_id = s.station_id "
                                 "AND t.last_reported = s.last_reported")
         .whenNotMatchedInsertAll()
         .execute())
        maintain(spark, status_path, batch_id)

    def upsert_stations(batch, batch_id):
        rows = latest_info(parse_info(batch))
        if not DeltaTable.isDeltaTable(spark, stations_path):
            rows.write.format("delta").save(stations_path)
            return
        (DeltaTable.forPath(spark, stations_path).alias("t")
         .merge(rows.alias("s"), "t.station_id = s.station_id")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())

    status = read_topic(spark, "velib.station_status")
    info = read_topic(spark, "velib.station_information")

    for name, stream in (("station_status", status), ("station_information", info)):
        (bronze(stream).writeStream.format("delta")
         .option("checkpointLocation", f"{LAKE}/_checkpoints/bronze_{name}")
         .partitionBy("date")
         .trigger(processingTime=TRIGGER)
         .start(f"{LAKE}/bronze/{name}"))

    (status.writeStream.foreachBatch(upsert_status)
     .option("checkpointLocation", f"{LAKE}/_checkpoints/silver_station_status")
     .trigger(processingTime=TRIGGER).start())
    (info.writeStream.foreachBatch(upsert_stations)
     .option("checkpointLocation", f"{LAKE}/_checkpoints/silver_stations")
     .trigger(processingTime=TRIGGER).start())

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
