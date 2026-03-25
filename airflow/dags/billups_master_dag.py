"""Master DAG: Bronze → Silver → Gold (sequential)."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from tasks.spark_tasks import run_module

default_args = {
    "owner": "billups",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="billups_medallion_master",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["billups", "spark", "medallion"],
) as dag:
    bronze = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=lambda: run_module("pipelines.bronze_ingestion"),
    )
    silver = PythonOperator(
        task_id="silver_builder",
        python_callable=lambda: run_module("pipelines.silver_builder"),
    )
    gold = PythonOperator(
        task_id="gold_all_questions",
        python_callable=lambda: run_module("pipelines.runner", extra_args=["--all"]),
    )
    bronze >> silver >> gold
