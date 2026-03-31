from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd

def create_table():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    hook.run("""
        CREATE TABLE IF NOT EXISTS temperature_data (
            noted_date date,
            temp numeric
        );
    """)

def full_load():
    df = pd.read_csv("/opt/airflow/output/transformed_temperature.csv")
    hook = PostgresHook(postgres_conn_id="postgres_default")
    hook.run("TRUNCATE TABLE temperature_data;")
    rows = [tuple(x) for x in df[["noted_date", "temp"]].to_numpy()]
    hook.insert_rows(table="temperature_data", rows=rows)

def incremental_load():
    df = pd.read_csv("/opt/airflow/output/transformed_temperature.csv")
    df["noted_date"] = pd.to_datetime(df["noted_date"])
    max_date = df["noted_date"].max()
    border_date = max_date - timedelta(days=3)
    df = df[df["noted_date"] >= border_date].copy()
    df["noted_date"] = df["noted_date"].dt.date

    hook = PostgresHook(postgres_conn_id="postgres_default")
    rows = [tuple(x) for x in df[["noted_date", "temp"]].to_numpy()]
    hook.insert_rows(table="temperature_data", rows=rows)

with DAG(
    dag_id="temperature_load_hw4",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    task_create_table = PythonOperator(
        task_id="create_table",
        python_callable=create_table
    )

    task_full_load = PythonOperator(
        task_id="full_load",
        python_callable=full_load
    )

    task_incremental_load = PythonOperator(
        task_id="incremental_load",
        python_callable=incremental_load
    )

    task_create_table >> task_full_load >> task_incremental_load

