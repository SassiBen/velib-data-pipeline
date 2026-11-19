from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import velib_tasks

with DAG(
    dag_id="velib_hourly",
    description="Contrôle de fraîcheur, modèles dbt, métriques et export gold",
    schedule="5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["velib"],
) as dag:
    freshness = PythonOperator(task_id="check_freshness",
                               python_callable=velib_tasks.check_freshness)
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd /opt/dbt && /opt/dbt-venv/bin/dbt build --profiles-dir . --target lake",
    )
    metrics = PythonOperator(task_id="publish_metrics",
                             python_callable=velib_tasks.publish_metrics)
    export = PythonOperator(task_id="export_gold", python_callable=velib_tasks.export_gold)

    freshness >> dbt_build >> [metrics, export]
