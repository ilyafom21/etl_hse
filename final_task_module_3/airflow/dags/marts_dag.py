from datetime import datetime
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator


def build_user_activity_mart():
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS mart;

        CREATE TABLE IF NOT EXISTS mart.user_activity (
            user_id VARCHAR(50) PRIMARY KEY,
            sessions_count INT,
            avg_session_duration_sec NUMERIC,
            total_pages_visited INT,
            total_actions INT
        );

        TRUNCATE TABLE mart.user_activity;

        INSERT INTO mart.user_activity (
            user_id,
            sessions_count,
            avg_session_duration_sec,
            total_pages_visited,
            total_actions
        )
        SELECT
            us.user_id,
            COUNT(DISTINCT us.session_id) AS sessions_count,
            AVG(EXTRACT(EPOCH FROM (us.end_time - us.start_time))) AS avg_session_duration_sec,
            COUNT(DISTINCT sp.session_id || '_' || sp.page_order) AS total_pages_visited,
            COUNT(DISTINCT sa.session_id || '_' || sa.action_order) AS total_actions
        FROM etl.user_sessions us
        LEFT JOIN etl.session_pages sp
            ON us.session_id = sp.session_id
        LEFT JOIN etl.session_actions sa
            ON us.session_id = sa.session_id
        GROUP BY us.user_id;
    """)

    conn.commit()
    cur.close()
    conn.close()


def build_support_stats_mart():
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS mart;

        CREATE TABLE IF NOT EXISTS mart.support_stats (
            status VARCHAR(30),
            issue_type VARCHAR(50),
            tickets_count INT,
            avg_resolution_hours NUMERIC
        );

        TRUNCATE TABLE mart.support_stats;

        INSERT INTO mart.support_stats (
            status,
            issue_type,
            tickets_count,
            avg_resolution_hours
        )
        SELECT
            status,
            issue_type,
            COUNT(*) AS tickets_count,
            AVG(EXTRACT(EPOCH FROM (updated_at - created_at)) / 3600.0) AS avg_resolution_hours
        FROM etl.support_tickets
        GROUP BY status, issue_type;
    """)

    conn.commit()
    cur.close()
    conn.close()


with DAG(
    dag_id="marts_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mart", "analytics"],
) as dag:

    user_activity_task = PythonOperator(
        task_id="build_user_activity_mart",
        python_callable=build_user_activity_mart,
    )

    support_stats_task = PythonOperator(
        task_id="build_support_stats_mart",
        python_callable=build_support_stats_mart,
    )

    [user_activity_task, support_stats_task]
