from datetime import datetime
from pymongo import MongoClient
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator


def mongo_to_postgres_etl():
    mongo_client = MongoClient("mongodb://mongo:27017/")
    mongo_db = mongo_client["etl_project"]

    sessions = list(mongo_db["UserSessions"].find({}, {"_id": 0}))
    events = list(mongo_db["EventLogs"].find({}, {"_id": 0}))
    tickets = list(mongo_db["SupportTickets"].find({}, {"_id": 0}))

    pg_conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="airflow",
        user="airflow",
        password="airflow"
    )
    cur = pg_conn.cursor()

    for session in sessions:
        cur.execute(
            """
            INSERT INTO etl.user_sessions (
                session_id, user_id, start_time, end_time, device
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (session_id) DO UPDATE
            SET
                user_id = EXCLUDED.user_id,
                start_time = EXCLUDED.start_time,
                end_time = EXCLUDED.end_time,
                device = EXCLUDED.device
            """,
            (
                session["session_id"],
                session["user_id"],
                session["start_time"],
                session["end_time"],
                session["device"],
            ),
        )

        cur.execute(
            "DELETE FROM etl.session_pages WHERE session_id = %s",
            (session["session_id"],)
        )

        for i, page in enumerate(session["pages_visited"], start=1):
            cur.execute(
                """
                INSERT INTO etl.session_pages (session_id, page_order, page_path)
                VALUES (%s, %s, %s)
                """,
                (session["session_id"], i, page),
            )

        cur.execute(
            "DELETE FROM etl.session_actions WHERE session_id = %s",
            (session["session_id"],)
        )

        for i, action in enumerate(session["actions"], start=1):
            cur.execute(
                """
                INSERT INTO etl.session_actions (session_id, action_order, action_name)
                VALUES (%s, %s, %s)
                """,
                (session["session_id"], i, action),
            )

    for event in events:
        cur.execute(
            """
            INSERT INTO etl.event_logs (
                event_id, event_timestamp, event_type, details
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE
            SET
                event_timestamp = EXCLUDED.event_timestamp,
                event_type = EXCLUDED.event_type,
                details = EXCLUDED.details
            """,
            (
                event["event_id"],
                event["timestamp"],
                event["event_type"],
                event["details"],
            ),
        )

    for ticket in tickets:
        cur.execute(
            """
            INSERT INTO etl.support_tickets (
                ticket_id, user_id, status, issue_type, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticket_id) DO UPDATE
            SET
                user_id = EXCLUDED.user_id,
                status = EXCLUDED.status,
                issue_type = EXCLUDED.issue_type,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at
            """,
            (
                ticket["ticket_id"],
                ticket["user_id"],
                ticket["status"],
                ticket["issue_type"],
                ticket["created_at"],
                ticket["updated_at"],
            ),
        )

        cur.execute(
            "DELETE FROM etl.ticket_messages WHERE ticket_id = %s",
            (ticket["ticket_id"],)
        )

        for i, msg in enumerate(ticket["messages"], start=1):
            cur.execute(
                """
                INSERT INTO etl.ticket_messages (
                    ticket_id, message_order, sender, message, message_timestamp
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    ticket["ticket_id"],
                    i,
                    msg["sender"],
                    msg["message"],
                    msg["timestamp"],
                ),
            )

    pg_conn.commit()
    cur.close()
    pg_conn.close()
    mongo_client.close()


with DAG(
    dag_id="mongo_to_postgres_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "mongo", "postgres"],
) as dag:

    etl_task = PythonOperator(
        task_id="mongo_to_postgres_etl",
        python_callable=mongo_to_postgres_etl,
    )

    etl_task
