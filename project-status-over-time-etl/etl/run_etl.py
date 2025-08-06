import subprocess
import os
import sys

POPULATE_SCRIPT = "etl/populate_projects.py"
TRANSFORM_SCRIPT = "etl/transform_daily_status.py"
EXPORT_SCRIPT = "etl/export_to_csv.py"

def run_script(script_path):
    print(f"Running {script_path}...")
    result = subprocess.run([sys.executable, script_path])
    if result.returncode != 0:
        raise Exception(f"{script_path} failed.")

def main():
    print("Starting ETL pipeline...\n")
    run_script(POPULATE_SCRIPT)
    run_script(TRANSFORM_SCRIPT)
    run_script(EXPORT_SCRIPT)
    print("\nETL pipeline completed and data exported to CSV.")

if __name__ == "__main__":
    main()