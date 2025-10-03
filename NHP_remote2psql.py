import os
import uuid
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from psycopg2.extras import execute_batch

# Load DB configs from .env file
load_dotenv()
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# === CONFIG ===
CSV_FOLDER = r"D:\MyWorkspace\Data Sets\NHP_rtdas"   # change path accordingly
TABLE_NAME = "nhp_rtdas_ingest"     # target table

def connect_db():
    return psycopg2.connect(**DB_CONFIG)

def generate_uuid(row):
    """Generate deterministic UUID based on row content for duplicate check."""
    row_str = "|".join(map(str, row.values))
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, row_str))

def ingest_csv(file_path):
    print(f"Processing: {file_path}")
    df = pd.read_csv(file_path)

    # Drop last two "spare" columns if present
    if df.shape[1] > 2:
        df = df.iloc[:, :-2]

    # Add UUID column
    df["uuid"] = df.apply(generate_uuid, axis=1)

    # Insert into DB
    with connect_db() as conn:
        with conn.cursor() as cur:
            # Create table if not exists
            columns = ", ".join([f'"{c}" TEXT' for c in df.columns])
            create_table = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                {columns},
                PRIMARY KEY (uuid)
            );
            """
            cur.execute(create_table)

            # Prepare insert
            cols = ", ".join([f'"{c}"' for c in df.columns])
            placeholders = ", ".join(["%s"] * len(df.columns))
            insert_sql = f"INSERT INTO {TABLE_NAME} ({cols}) VALUES ({placeholders}) ON CONFLICT (uuid) DO NOTHING"

            execute_batch(cur, insert_sql, df.values.tolist(), page_size=500)

    print(f"Inserted {len(df)} records from {os.path.basename(file_path)}")

def ingest_all_csv(folder_path):
    for file in os.listdir(folder_path):
        if file.endswith(".csv"):
            try:
                ingest_csv(os.path.join(folder_path, file))
            except Exception as e:
                print(f"❌ Failed for {file}: {e}")

if __name__ == "__main__":
    ingest_all_csv(CSV_FOLDER)
