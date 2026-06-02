from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreatePysparkJobOperator,
)

CLUSTER_ID = "c9qcvmpakqtlol00si1o"
YC_BUCKET = "etl-homework-bucket-12345"

with DAG(
    dag_id="final_etl_dataproc_dag",
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
) as dag:

    run_pyspark_job = DataprocCreatePysparkJobOperator(
        task_id="run_pyspark_job",
        cluster_id=CLUSTER_ID,
        main_python_file_uri=f"s3a://{YC_BUCKET}/process_face_data.py",
    )