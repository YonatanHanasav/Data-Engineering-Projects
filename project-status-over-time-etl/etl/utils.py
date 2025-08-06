import psycopg2
from datetime import datetime

def log_etl_status(conn, etl_stage, status, row_count=None, error_message=None):
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO etl_audit_log (etl_stage, status, row_count, run_timestamp, error_message)
            VALUES (%s, %s, %s, %s, %s)
        """, (etl_stage, status, row_count, datetime.now(), error_message))
    conn.commit()