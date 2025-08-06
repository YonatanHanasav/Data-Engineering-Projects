import psycopg2
from dotenv import load_dotenv
import os
from datetime import datetime

# Load environment variables from .env
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

SQL_FILE_PATH = "sql/transform_projects_to_daily_status.sql"

def log_etl_status(conn, etl_stage, status, row_count=None, error_message=None):
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO etl_audit_log (etl_stage, status, row_count, run_timestamp, error_message)
            VALUES (%s, %s, %s, %s, %s)
        """, (etl_stage, status, row_count, datetime.now(), error_message))
    conn.commit()

def run_sql_file():
    conn = None
    cursor = None
    try:
        # Read SQL content from file
        with open(SQL_FILE_PATH, "r") as file:
            sql = file.read()

        # Connect to PostgreSQL using .env credentials
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        # Execute the transformation SQL
        cursor.execute(sql)
        conn.commit()

        # Count rows inserted (optional: adjust if logic overwrites)
        cursor.execute("SELECT COUNT(*) FROM daily_project_status")
        row_count = cursor.fetchone()[0]

        log_etl_status(conn, "transform_daily_status", "success", row_count=row_count)
        print("Daily status transformation completed.")

    except Exception as e:
        if conn:
            log_etl_status(conn, "transform_daily_status", "failed", error_message=str(e))
        print("Error during transformation:")
        print(e)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    run_sql_file()