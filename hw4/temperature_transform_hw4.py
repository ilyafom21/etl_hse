from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def transform_temperature_data():
    df = pd.read_csv("/opt/airflow/dags/data/IOT-temp.csv")

    df = df[df["out/in"] == "In"].copy()
    df["noted_date"] = pd.to_datetime(df["noted_date"], format="%d-%m-%Y %H:%M").dt.date

    q5 = df["temp"].quantile(0.05)
    q95 = df["temp"].quantile(0.95)

    df = df[(df["temp"] >= q5) & (df["temp"] <= q95)]

    df.to_csv("/opt/airflow/output/transformed_temperature.csv", index=False)

with DAG(
    dag_id="temperature_transform_hw4",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    task = PythonOperator(
        task_id="transform_temperature_data",
        python_callable=transform_temperature_data
    )
