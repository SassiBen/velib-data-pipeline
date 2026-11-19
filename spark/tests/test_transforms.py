import json
import sys
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

from pyspark.sql import SparkSession  # noqa: E402

from transforms import dedup_status, latest_info, parse_info, parse_status  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("tests") \
        .config("spark.sql.session.timeZone", "UTC").getOrCreate()
    yield s
    s.stop()


def kafka_rows(spark, payloads):
    rows = [(json.dumps(p).encode(),) for p in payloads]
    return spark.createDataFrame(rows, "value binary")


STATUS = {"schema": 1, "station_id": "213688169", "station_code": "16107", "bikes": 11,
          "mechanical": 3, "ebike": 8, "docks": 24, "is_installed": True, "is_renting": True,
          "is_returning": True, "last_reported": 1795047571, "polled_at": 1795051082}


def test_parse_status_types_and_dates(spark):
    df = parse_status(kafka_rows(spark, [STATUS]))
    row = df.collect()[0]
    assert row.bikes == 11 and row.ebike == 8
    assert str(row.date) == "2026-11-19"
    assert "schema" not in df.columns


def test_unknown_schema_versions_are_ignored(spark):
    df = parse_status(kafka_rows(spark, [STATUS, {**STATUS, "schema": 2}, {"junk": 1}]))
    assert df.count() == 1


def test_duplicates_collapse(spark):
    df = dedup_status(parse_status(kafka_rows(spark, [STATUS, STATUS,
                                                      {**STATUS, "last_reported": 1795047631}])))
    assert df.count() == 2


def test_latest_info_wins(spark):
    a = {"schema": 1, "station_id": "1", "station_code": "16107", "name": "Ancien nom",
         "lat": 48.86, "lon": 2.27, "capacity": 30, "polled_at": 100}
    b = {**a, "name": "Nouveau nom", "capacity": 35, "polled_at": 200}
    rows = latest_info(parse_info(kafka_rows(spark, [a, b]))).collect()
    assert len(rows) == 1 and rows[0].name == "Nouveau nom"
