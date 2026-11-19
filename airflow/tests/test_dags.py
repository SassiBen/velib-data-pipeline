import sys
from pathlib import Path

import pytest

DAGS = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS))


def test_lag_and_metric_names():
    import velib_tasks

    assert velib_tasks.lag_seconds(1_000_000, now=1_060.0) == 60.0

    class FakeCon:
        def execute(self, sql):
            return self

        def fetchone(self):
            return (1500, 21000, 9000, 80, 35)

    m = velib_tasks.network_metrics(FakeCon())
    assert m["velib_network_empty_stations"] == 80.0
    assert all(k.startswith("velib_network_") for k in m)


def test_dag_structure():
    airflow = pytest.importorskip("airflow")
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(DAGS), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    dag = bag.get_dag("velib_hourly")
    assert dag is not None and airflow
    assert dag.get_task("dbt_build").upstream_task_ids == {"check_freshness"}
    assert dag.get_task("export_gold").upstream_task_ids == {"dbt_build"}
