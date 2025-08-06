import os
import psycopg2
import csv
from dotenv import load_dotenv
from datetime import datetime

# Load credentials from .env
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Create output folder and file name with date
OUTPUT_DIR = "data_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

today_str = datetime.today().strftime("%Y%m%d")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, f"daily_status_{today_str}.csv")

def export_to_csv():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = conn.cursor()

    cursor.execute("SELECT project_id, project_date, status FROM daily_project_status")
    rows = cursor.fetchall()

    with open(OUTPUT_CSV, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["project_id", "project_date", "status"])
        writer.writerows(rows)

    cursor.close()
    conn.close()
    print(f"Exported to {OUTPUT_CSV}")

if __name__ == "__main__":
    export_to_csv()