from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

PROJECT = "/workspaces/de-taxi-pipeline"

with DAG(
    dag_id="taxi_pipeline",
    description="NYC taxi ETL: clean -> aggregate -> load -> BigQuery",
    start_date=datetime(2024, 1, 1),
    schedule=None,          # trigger manually
    catchup=False,
    tags=["portfolio", "etl"],
) as dag:

    clean = BashOperator(
        task_id="clean",
        bash_command=f"cd {PROJECT} && python clean.py",
    )

    aggregate = BashOperator(
        task_id="aggregate",
        bash_command=f"cd {PROJECT} && python aggregate.py",
    )

    load_csv = BashOperator(
        task_id="load_csv",
        bash_command=f"cd {PROJECT} && python load_bq.py",
    )

    load_bigquery = BashOperator(
        task_id="load_bigquery",
        bash_command=(
            f"cd {PROJECT} && bq load --autodetect --source_format=CSV "
            f"--skip_leading_rows=1 --replace "
            f"taxi_analytics.daily_metrics out_agg/part-*.csv"
        ),
    )

    # dependency chain — each runs only if the previous succeeded
    clean >> aggregate >> load_csv >> load_bigquery
