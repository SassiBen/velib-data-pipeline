"""Tâches Python des DAG Vélib'. Gardées hors du fichier de DAG pour être testables."""

import os
import time

LAKE = os.getenv("LAKE_URI_S3", "s3://lake")
WAREHOUSE = os.getenv("WAREHOUSE_PATH", "/data/warehouse/velib.duckdb")
PUSHGATEWAY = os.getenv("PUSHGATEWAY", "pushgateway:9091")
MAX_LAG_SECONDS = int(os.getenv("MAX_LAG_SECONDS", "1800"))


def storage_options() -> dict:
    return {
        "AWS_ENDPOINT_URL": f"http://{os.getenv('S3_ENDPOINT_HOST', 'garage:3900')}",
        "AWS_ACCESS_KEY_ID": os.environ["S3_ACCESS_KEY"],
        "AWS_SECRET_ACCESS_KEY": os.environ["S3_SECRET_KEY"],
        "AWS_REGION": os.getenv("S3_REGION", "garage"),
        "AWS_ALLOW_HTTP": "true",
    }


def lag_seconds(last_commit_ms: int, now: float | None = None) -> float:
    return (now or time.time()) - last_commit_ms / 1000


def push(metrics: dict[str, float], job: str) -> None:
    from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

    registry = CollectorRegistry()
    for name, value in metrics.items():
        Gauge(name, name, registry=registry).set(value)
    push_to_gateway(PUSHGATEWAY, job=job, registry=registry)


def check_freshness(**_) -> float:
    """Échoue si la couche bronze n'a rien reçu depuis plus de MAX_LAG_SECONDS.

    On surveille bronze et non silver : silver n'est écrite que lorsque le flux
    publie des relevés nouveaux, ce qui peut prendre vingt minutes alors que
    tout fonctionne. La première version surveillait silver et a déclenché une
    fausse alerte dès le premier lancement.
    """
    from deltalake import DeltaTable

    table = DeltaTable(f"{LAKE}/bronze/station_status", storage_options=storage_options())
    lag = lag_seconds(table.history(1)[0]["timestamp"])
    push({"velib_bronze_lag_seconds": lag}, job="velib_freshness")
    if lag > MAX_LAG_SECONDS:
        raise RuntimeError(f"bronze sans écriture depuis {lag:.0f} s (seuil {MAX_LAG_SECONDS} s)")
    return lag


NETWORK_SQL = """
select
    count(*)                                 as stations,
    sum(bikes)                               as bikes,
    sum(ebike)                               as ebikes,
    sum(case when is_empty then 1 else 0 end) as empty_stations,
    sum(case when is_full then 1 else 0 end)  as full_stations
from mart_network_now
"""


def network_metrics(con) -> dict[str, float]:
    row = con.execute(NETWORK_SQL).fetchone()
    keys = ["stations", "bikes", "ebikes", "empty_stations", "full_stations"]
    return {f"velib_network_{k}": float(v or 0) for k, v in zip(keys, row, strict=True)}


def publish_metrics(**_) -> dict:
    import duckdb

    with duckdb.connect(WAREHOUSE, read_only=True) as con:
        metrics = network_metrics(con)
    push(metrics, job="velib_network")
    return metrics


def export_gold(**_) -> list[str]:
    """Copie les tables de la couche gold en Parquet dans le lac, pour d'autres outils."""
    import duckdb

    host = os.getenv("S3_ENDPOINT_HOST", "garage:3900")
    written = []
    with duckdb.connect(WAREHOUSE, read_only=True) as con:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"""CREATE SECRET lake (TYPE s3, KEY_ID '{os.environ["S3_ACCESS_KEY"]}',
            SECRET '{os.environ["S3_SECRET_KEY"]}', ENDPOINT '{host}', URL_STYLE 'path',
            USE_SSL false, REGION '{os.getenv("S3_REGION", "garage")}')""")
        for table in ("dim_stations", "mart_saturation", "mart_zone_hourly"):
            target = f"{LAKE}/gold/{table}.parquet"
            con.execute(f"COPY {table} TO '{target}' (FORMAT parquet)")
            written.append(target)
    return written
