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

    daily_temp = df.groupby("noted_date", as_index=False)["temp"].mean()

    hottest = daily_temp.sort_values("temp", ascending=False).head(5)
    coldest = daily_temp.sort_values("temp", ascending=True).head(5)

    hottest.to_csv("/opt/airflow/output/hottest_days.csv", index=False)
    coldest.to_csv("/opt/airflow/output/coldest_days.csv", index=False)

with DAG(
    dag_id="temperature_etl_hw",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    task = PythonOperator(
        task_id="transform_temperature_data",
        python_callable=transform_temperature_data
    )
