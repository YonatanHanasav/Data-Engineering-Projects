import uuid
import random
import datetime
import psycopg2
from faker import Faker
from dotenv import load_dotenv
import os

# ---- Load environment variables from .env ----
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

NUM_PROJECTS = 500
REGIONS = ['US', 'EU', 'APAC', 'LATAM', 'MEA']
OWNERS = ['Engineering', 'Marketing', 'Operations', 'PMO', 'R&D']

fake = Faker()

# ---- Connect to PostgreSQL ----
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
cursor = conn.cursor()

# ---- Insert Synthetic Projects ----
insert_query = """
    INSERT INTO projects (
        project_id,
        store_date,
        initial_date,
        active_date,
        end_date,
        project_name,
        owner,
        region,
        budget
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

today = datetime.date.today()

for _ in range(NUM_PROJECTS):
    project_id = str(uuid.uuid4())

    # 1. Decide if the project will have initial_date
    has_initial = random.random() > 0.05  # 95% of projects will have initial_date

    initial_date = None
    active_date = None
    end_date = None

    if has_initial:
        initial_date = fake.date_between(start_date="-3y", end_date="-1y")

        # store_date must be before initial_date
        store_date = initial_date - datetime.timedelta(days=random.randint(1, 60))

        # 2. Decide if it will have active_date
        has_active = random.random() > 0.2  # 80% chance to have active_date

        if has_active:
            active_date = initial_date + datetime.timedelta(days=random.randint(5, 30))

            # 3. Decide if it will have end_date
            has_end = random.random() > 0.25  # 75% chance to have end_date if active exists

            if has_end:
                end_date = active_date + datetime.timedelta(days=random.randint(30, 120))

    else:
        # If no initial_date, no active or end are allowed
        store_date = fake.date_between(start_date="-3y", end_date="-1y")

    project_name = fake.bs().title()
    owner = random.choice(OWNERS)
    region = random.choice(REGIONS)
    budget = round(random.uniform(10000, 500000), 2)

    cursor.execute(insert_query, (
        project_id,
        store_date,
        initial_date,
        active_date,
        end_date,
        project_name,
        owner,
        region,
        budget
    ))

conn.commit()
cursor.close()
conn.close()

print(f"Successfully inserted {NUM_PROJECTS} synthetic projects.")