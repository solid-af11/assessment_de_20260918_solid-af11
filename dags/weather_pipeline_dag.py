"""
Daily weather pipeline DAG.

extract -> load -> dbt run -> dbt test

Uses the logical date (data_interval_start) so `airflow dags backfill` works
correctly — never hard-codes "today".
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingestion.extract import fetch_city_range
from ingestion.load import load_records_for_date
from ingestion.run import load_cities

DBT_PROJECT_DIR = "/opt/airflow/dbt"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
}


def extract_task(logical_date, ti, **_):
    """Fetch weather for all configured cities for the logical date, push to XCom."""
    target_date = logical_date.date()
    cities = load_cities()
    all_records = {}

    for city in cities:
        records = fetch_city_range(
            city_name=city["name"],
            latitude=city["latitude"],
            longitude=city["longitude"],
            timezone=city["timezone"],
            start_date=target_date,
            end_date=target_date,
        )
        all_records[city["name"]] = records

    ti.xcom_push(key="records", value=all_records)


def load_task(logical_date, ti, **_):
    """Load the records extracted by extract_task into raw.weather_daily."""
    target_date = logical_date.date()
    all_records = ti.xcom_pull(task_ids="extract", key="records")

    results = {}
    for city_name, records in all_records.items():
        count = load_records_for_date(city_name, target_date, records)
        results[city_name] = count

    return results


with DAG(
    dag_id="weather_pipeline",
    description="Extract-load-transform daily weather from Open-Meteo into the warehouse.",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    tags=["weather", "assessment"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_task,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_task,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
    )

    extract >> load >> dbt_run >> dbt_test
