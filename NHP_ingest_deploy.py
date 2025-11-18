import os
import uuid
import psycopg2
import pandas as pd
import logging
import re
import json
from datetime import datetime
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
from multiprocessing import Pool, cpu_count

# ===============================================================
# CONFIG
# ===============================================================
load_dotenv()
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

CSV_FOLDER = r"D:\FTP_SYNC_DATA\NHP_FTP_SYNC"
TABLE_NAME = "nhp_rtdas_ingest_v1"
AUDIT_TABLE = "nhp_rtdas_ingest_audit_v1"
PROCESSED_TABLE = "nhp_ingest_files"   # table that records processed files

EXPECTED_COLUMNS = [
    "StationID", "DateTime", "MobileNumber", "Battery", "WaterLevel",
    "HourlyRain", "DailyRain", "AT", "SnowDepth",
    "Evaporation", "WS", "WD", "At.pressure", "RH", "Sun Radiation"
]

# ===============================================================
# LOGGING
# ===============================================================
LOG_FILE = "rtdas_ingest.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# ===============================================================
# DB CONNECTION
# ===============================================================
def connect_db():
    return psycopg2.connect(**DB_CONFIG)

# ===============================================================
# HELPERS: UUID, safe CSV read, normalize headers, validators
# ===============================================================
def generate_uuid(row):
    row_str = "|".join(map(str, row.values))
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, row_str))


def safe_read_csv(file_path):
    """
    Read CSV robustly:
    - Remove NUL bytes
    - Use python engine to allow on_bad_lines collector
    - Return (DataFrame, bad_rows_list)
    """
    bad_rows = []
    from io import StringIO

    cleaned_lines = []
    with open(file_path, "rb") as f:
        for raw in f:
            cleaned = raw.replace(b"\x00", b"")
            try:
                cleaned_lines.append(cleaned.decode("utf-8", errors="ignore"))
            except Exception:
                # ignore lines that cannot be decoded
                continue

    buffer = StringIO("".join(cleaned_lines))

    # collect bad lines (as list)
    def bad_line_collector(line):
        bad_rows.append(line)
        return None

    df = pd.read_csv(
        buffer,
        dtype=str,
        header=None,
        engine="python",
        on_bad_lines=bad_line_collector
    )

    return df, bad_rows


# header normalization map
HEADER_MAP = {
    "stationid": "StationID", "station": "StationID", "stid": "StationID", "stnid": "StationID",
    "dateandtime": "DateTime", "datetime": "DateTime", "date_time": "DateTime", "datetimestamp": "DateTime",
    "date": "Date", "time": "Time",
    "mobilenumber": "MobileNumber", "mobile": "MobileNumber", "mob": "MobileNumber",
    "battery": "Battery", "batt": "Battery", "batteryvolt": "Battery", "batteryvoltage": "Battery",
    "waterlevel": "WaterLevel", "wl": "WaterLevel",
    "hourlyrain": "HourlyRain", "hourrain": "HourlyRain",
    "dailyrain": "DailyRain", "dailyrainfall": "DailyRain", "rain24": "DailyRain",
    "at": "AT", "airtemp": "AT", "temp": "AT",
    "snowdepth": "SnowDepth", "snow": "SnowDepth",
    "evaporation": "Evaporation", "evap": "Evaporation",
    "ws": "WS", "wd": "WD",
    "atpressure": "At.pressure", "pressure": "At.pressure", "baro": "At.pressure",
    "rh": "RH", "humidity": "RH",
    "sunradiation": "Sun Radiation", "solar": "Sun Radiation", "radiation": "Sun Radiation",
}


def normalize_headers(cols):
    normalized = []
    for c in cols:
        if not isinstance(c, str):
            normalized.append(c)
            continue
        name = c.strip().replace("&", "").replace(" ", "").replace("_", "").lower()
        normalized.append(HEADER_MAP.get(name, c.strip()))
    return normalized


# ===============================================================
# STRICT VALIDATORS (user requested)
# ===============================================================
stationid_re = re.compile(r"^&[a-fA-F0-9]{8}$")
# DateTime: DD-MM-YYYY HH:MM:SS  OR  YYYY-MM-DD HH:MM:SS
dt_re_list = [
    re.compile(r"^\d{2}[-/]\d{2}[-/]\d{2} \d{2}:\d{2}(:\d{2})?$"),  # DD-MM-YY/ YY-MM-DD HH:MM:SS HH:MM
    re.compile(r"^(\d{2}[-/]\d{2}[-/]\d{4}|\d{4}[-/]\d{2}[-/]\d{2}) "  
    r"\d{2}:\d{2}(:\d{2})?$")
]

def valid_stationid(val: str) -> bool:
    if not isinstance(val, str):
        return False
    return bool(stationid_re.fullmatch(val.strip()))


def valid_datetime(val: str) -> bool:
    if not isinstance(val, str):
        return False
    s = val.strip()
    for pat in dt_re_list:
        if pat.fullmatch(s):
            return True
    return False


def is_valid_record_strict(row):
    """
    Strict check: both StationID and DateTime must match required patterns.
    If either fails, the row is invalid and will be skipped.
    """
    sid = str(row.get("StationID", "")).strip()
    dt = str(row.get("DateTime", "")).strip()

    if not sid or not dt:
        return False
    if not valid_stationid(sid):
        return False
    if not valid_datetime(dt):
        return False
    return True


# ===============================================================
# DB TABLE CREATION (including processed-files table)
# ===============================================================
def ensure_tables():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cols = ", ".join([f'"{c}" TEXT' for c in EXPECTED_COLUMNS + ["uuid"]])
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                    {cols},
                    PRIMARY KEY (uuid)
                );
            """)

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
                    id SERIAL PRIMARY KEY,
                    file_name TEXT,
                    record_count INT,
                    success_count INT,
                    fail_count INT,
                    failed_records JSONB,
                    timestamp TIMESTAMP DEFAULT NOW()
                );
            """)

            # table to record processed file names (Option 1)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {PROCESSED_TABLE} (
                    file_name TEXT PRIMARY KEY,
                    processed_at TIMESTAMP DEFAULT NOW()
                );
            """)
        conn.commit()


def already_processed_set():
    """Return set of filenames already recorded as processed."""
    s = set()
    try:
        with connect_db() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT file_name FROM {PROCESSED_TABLE}")
                rows = cur.fetchall()
                s = set(r[0] for r in rows)
    except Exception as e:
        logging.warning(f"Could not fetch processed files list: {e}")
    return s


def mark_file_processed(cur, file_name):
    """Insert into processed files table using provided cursor (so part of same txn)."""
    cur.execute(
        f"INSERT INTO {PROCESSED_TABLE} (file_name, processed_at) VALUES (%s, NOW()) ON CONFLICT (file_name) DO UPDATE SET processed_at = EXCLUDED.processed_at",
        (file_name,)
    )


# ===============================================================
# INGEST WORKER (single-file processing) - used by multiprocessing pool
# ===============================================================
def ingest_csv(file_path):
    file_name = os.path.basename(file_path)
    failed_records = []
    inserted_count = 0
    print(f"Processing: {file_name}")

    try:
        # Robust read that skips only malformed lines
        df_raw, bad_rows = safe_read_csv(file_path)

        # Record tokenizing problems as failed_records (but continue)
        if bad_rows:
            failed_records.extend([{"error": "tokenize", "raw": str(r)} for r in bad_rows])
            logging.error(f"{file_name}: {len(bad_rows)} tokenizing rows skipped.")

        # Header detection and assignment
        first_row = df_raw.iloc[0].astype(str).tolist()
        first_row_lc = [str(x).lower() for x in first_row]
        has_header = any("station" in x or "date" in x for x in first_row_lc)

        if has_header:
            # treat first row as header: assign normalized header names
            df = df_raw[1:].copy()
            df.columns = normalize_headers(first_row)
        else:
            df = df_raw.copy()
            df.columns = EXPECTED_COLUMNS[:len(df.columns)]

        # Normalize column names (again to be safe)
        df.columns = normalize_headers(df.columns)

        # Merge Date + Time into DateTime if separate
        if "Date" in df.columns and "Time" in df.columns:
            df["DateTime"] = df["Date"].astype(str).str.strip() + " " + df["Time"].astype(str).str.strip()
            df = df.drop(columns=["Date", "Time"], errors="ignore")

        # Ensure expected columns exist (fill missing with None), trim extras
        for col in EXPECTED_COLUMNS:
            if col not in df.columns:
                df[col] = None
        df = df[EXPECTED_COLUMNS]

        # Drop rows that are entirely blank
        df = df.dropna(how="all")
        if df.empty and not failed_records:
            logging.warning(f"{file_name}: No usable rows found.")
            return {"file": file_name, "inserted": 0, "skipped": 0, "error": None}

        # STRICT validation: keep only records that satisfy both StationID and DateTime patterns
        valid_mask = df.apply(is_valid_record_strict, axis=1)
        invalid_rows = df[~valid_mask]
        if not invalid_rows.empty:
            # record examples and full failed rows in audit
            failed_records.extend(invalid_rows.to_dict(orient="records"))
            # log samples for quick debugging
            sample_bad = [str(r.get("StationID")) for r in invalid_rows.head(5).to_dict(orient="records")]
            logging.warning(f"{file_name}: {len(invalid_rows)} rows failed strict validation, examples: {sample_bad}")
            df = df[valid_mask]

        if df.empty:
            logging.error(f"{file_name}: All rows failed strict validation. Skipping file.")
            # insert audit entry (all failed)
            with connect_db() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        INSERT INTO {AUDIT_TABLE}
                        (file_name, record_count, success_count, fail_count, failed_records)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        file_name,
                        len(invalid_rows) + (len(bad_rows) if bad_rows else 0),
                        0,
                        len(invalid_rows) + (len(bad_rows) if bad_rows else 0),
                        json.dumps(failed_records) if failed_records else None,
                    ))
                    # mark as processed to avoid reprocessing garbage files
                    mark_file_processed(cur, file_name)
                conn.commit()
            return {"file": file_name, "inserted": 0, "skipped": len(failed_records), "error": "all_invalid"}

        # Add UUID column
        df["uuid"] = df.apply(generate_uuid, axis=1)

        # Insert valid rows and write audit + mark processed in same transaction
        with connect_db() as conn:
            with conn.cursor() as cur:
                cols = ", ".join([f'"{c}"' for c in df.columns])
                ph = ", ".join(["%s"] * len(df.columns))
                execute_batch(cur,
                              f"INSERT INTO {TABLE_NAME} ({cols}) VALUES ({ph}) ON CONFLICT (uuid) DO NOTHING",
                              df.values.tolist(),
                              page_size=500)
                inserted_count = len(df)

                # audit entry (only if there are failures or to record counts)
                cur.execute(f"""
                    INSERT INTO {AUDIT_TABLE}
                    (file_name, record_count, success_count, fail_count, failed_records)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    file_name,
                    inserted_count + len(failed_records),
                    inserted_count,
                    len(failed_records),
                    json.dumps(failed_records) if failed_records else None,
                ))

                # mark file processed
                mark_file_processed(cur, file_name)

            conn.commit()

        print(f"✅ {file_name}: {inserted_count} inserted, {len(failed_records)} skipped.")
        if failed_records:
            logging.info(f"{file_name}: {inserted_count} inserted, {len(failed_records)} skipped.")
        return {"file": file_name, "inserted": inserted_count, "skipped": len(failed_records), "error": None}

    except Exception as e:
        logging.error(f"{file_name}: {str(e)}")
        print(f"❌ Failed for {file_name}: {e}")
        return {"file": file_name, "inserted": 0, "skipped": 0, "error": str(e)}


# ===============================================================
# MAIN BATCH: multiprocessing + skip processed files
# ===============================================================
def ingest_all_csv(folder_path, max_workers=None):
    ensure_tables()
    all_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.lower().endswith(".csv")]

    # get already processed file names to skip
    processed = already_processed_set()
    files_to_process = [f for f in all_files if os.path.basename(f) not in processed]

    if not files_to_process:
        print("No new CSVs to process.")
        return

    # decide number of workers
    cpu = cpu_count()
    if max_workers is None:
        num_workers = min(4, max(1, cpu // 2))
    else:
        num_workers = max(1, min(max_workers, cpu))
    print(f"Starting ingestion on {len(files_to_process)} files with {num_workers} workers...")

    # Use a pool: map returns results that we can log/inspect
    with Pool(processes=num_workers) as pool:
        results = pool.map(ingest_csv, files_to_process)

    # summary
    total_inserted = sum(r.get("inserted", 0) for r in results)
    total_skipped = sum(r.get("skipped", 0) for r in results)
    errors = [r for r in results if r.get("error")]
    print(f"Done. Inserted {total_inserted} rows; Skipped {total_skipped} rows; Errors in {len(errors)} files.")
    if errors:
        logging.error(f"Errors: {json.dumps(errors, default=str)}")


# ===============================================================
# ENTRY POINT
# ===============================================================
if __name__ == "__main__":
    ingest_all_csv(CSV_FOLDER)