# import os
# import uuid
# import psycopg2
# import pandas as pd
# import logging
# import re
# from datetime import datetime
# from psycopg2.extras import execute_batch
# from dotenv import load_dotenv

# # ===============================================================
# # CONFIG
# # ===============================================================
# load_dotenv()
# DB_CONFIG = {
#     "host": os.getenv("DB_HOST"),
#     "port": os.getenv("DB_PORT"),
#     "dbname": os.getenv("DB_NAME"),
#     "user": os.getenv("DB_USER"),
#     "password": os.getenv("DB_PASSWORD"),
# }

# #CSV_FOLDER = r"D:\FTP_SYNC_DATA\NHP_FTP_SYNC"
# CSV_FOLDER = r"C:\Users\hp\Desktop\New folder"
# TABLE_NAME = "nhp_rtdas_ingest_test01"
# AUDIT_TABLE = "nhp_rtdas_ingest_audit_test01"

# EXPECTED_COLUMNS = [
#     "StationID", "DateTime", "MobileNumber", "Battery", "WaterLevel",
#     "HourlyRain", "DailyRain", "AT", "SnowDepth",
#     "Evaporation", "WS", "WD", "At.pressure", "RH", "Sun Radiation"
# ]

# # ===============================================================
# # LOGGING
# # ===============================================================
# LOG_FILE = "rtdas_ingest.log"
# logging.basicConfig(
#     filename=LOG_FILE,
#     level=logging.INFO,
#     format="%(asctime)s | %(levelname)s | %(message)s",
# )

# # ===============================================================
# # DB CONNECTION
# # ===============================================================
# def connect_db():
#     return psycopg2.connect(**DB_CONFIG)

# # ===============================================================
# # HELPERS
# # ===============================================================
# def generate_uuid(row):
#     """Generate deterministic UUID for deduplication."""
#     row_str = "|".join(map(str, row.values))
#     return str(uuid.uuid5(uuid.NAMESPACE_DNS, row_str))


# def looks_like_date(value: str) -> bool:
#     """Check if a value resembles a date or datetime."""
#     if not value:
#         return False
#     s = value.strip()

#     # Common date-like patterns
#     date_patterns = [
#         r"\d{4}[-/]\d{2}[-/]\d{2}",   # 2024-08-25 or 2024/08/25
#         r"\d{2}[-/]\d{2}[-/]\d{4}",   # 25-08-2024
#         r"\d{2}[-/]\d{2}[-/]\d{2}",   # 25-08-24
#         r"\d{4}\d{2}\d{2}",           # 20240825
#     ]
#     if any(re.fullmatch(p, s) for p in date_patterns):
#         return True

#     # Looks like datetime or contains timestamp-like tokens
#     if re.search(r"\d{2}:\d{2}", s) or re.search(r"\s\d{2}", s):
#         return True

#     return False


# def is_valid_record(row):
#     """Reject rows where StationID looks like a date or is empty or garbage."""
#     sid = str(row.get("StationID", "")).strip()
#     if not sid:
#         return False
#     if looks_like_date(sid):
#         return False
#     # Reject if numeric only or too short
#     if sid.isdigit() and len(sid) < 6:
#         return False
#     return True


# def normalize_headers(cols):
#     """Normalize messy CSV headers."""
#     normalized = []
#     for c in cols:
#         if not isinstance(c, str):
#             normalized.append(c)
#             continue
#         name = c.strip().replace("&", "").replace(" ", "").replace("_", "").lower()

#         if name in ["stationid", "station", "stid", "stnid"]:
#             normalized.append("StationID")
#         elif name in ["dateandtime", "datetime", "date_time", "datetimestamp"]:
#             normalized.append("DateTime")
#         elif name == "date":
#             normalized.append("Date")
#         elif name == "time":
#             normalized.append("Time")
#         elif name in ["mobilenumber", "mobile", "mob"]:
#             normalized.append("MobileNumber")
#         elif name in ["battery", "batt", "batteryvolt", "batteryvoltage"]:
#             normalized.append("Battery")
#         elif name in ["waterlevel", "wl"]:
#             normalized.append("WaterLevel")
#         elif name in ["hourlyrain", "hourrain"]:
#             normalized.append("HourlyRain")
#         elif name in ["dailyrain", "dailyrainfall", "rain24"]:
#             normalized.append("DailyRain")
#         elif name in ["at", "airtemp", "temp"]:
#             normalized.append("AT")
#         elif name in ["snowdepth", "snow"]:
#             normalized.append("SnowDepth")
#         elif name in ["evaporation", "evap"]:
#             normalized.append("Evaporation")
#         elif name == "ws":
#             normalized.append("WS")
#         elif name == "wd":
#             normalized.append("WD")
#         elif name in ["atpressure", "pressure", "baro"]:
#             normalized.append("At.pressure")
#         elif name in ["rh", "humidity"]:
#             normalized.append("RH")
#         elif name in ["sunradiation", "solar", "radiation"]:
#             normalized.append("Sun Radiation")
#         else:
#             normalized.append(c.strip())
#     return normalized


# # ===============================================================
# # DB TABLE CREATION
# # ===============================================================
# def ensure_tables():
#     """Ensure target and audit tables exist."""
#     with connect_db() as conn:
#         with conn.cursor() as cur:
#             cols = ", ".join([f'"{c}" TEXT' for c in EXPECTED_COLUMNS + ["uuid"]])
#             cur.execute(f"""
#                 CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
#                     {cols},
#                     PRIMARY KEY (uuid)
#                 );
#             """)
#             cur.execute(f"""
#                 CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
#                     id SERIAL PRIMARY KEY,
#                     file_name TEXT,
#                     record_count INT,
#                     success_count INT,
#                     fail_count INT,
#                     failed_records JSONB,
#                     timestamp TIMESTAMP DEFAULT NOW()
#                 );
#             """)
#         conn.commit()


# # ===============================================================
# # INGEST FUNCTION
# # ===============================================================
# def ingest_csv(file_path):
#     file_name = os.path.basename(file_path)
#     failed_records = []
#     print(f"Processing: {file_name}")

#     try:
#         # Try to read flexibly — handle missing or extra headers
#         df_raw = pd.read_csv(file_path, dtype=str, header=None, encoding_errors="ignore")

#         # Header detection
#         first_row = df_raw.iloc[0].astype(str).str.lower().tolist()
#         has_header = any("station" in x or "date" in x for x in first_row)

#         if has_header:
#             df = pd.read_csv(file_path, dtype=str, encoding_errors="ignore")
#         else:
#             df = df_raw.copy()
#             # Assign fixed headers up to number of columns
#             df.columns = EXPECTED_COLUMNS[:len(df.columns)]

#         # Normalize headers
#         df.columns = normalize_headers(df.columns)

#         # Merge Date + Time if necessary
#         if "Date" in df.columns and "Time" in df.columns:
#             df["DateTime"] = df["Date"].astype(str).str.strip() + " " + df["Time"].astype(str).str.strip()
#             df.drop(columns=["Date", "Time"], inplace=True)

#         # Handle missing or extra columns
#         for col in EXPECTED_COLUMNS:
#             if col not in df.columns:
#                 df[col] = None
#         df = df[EXPECTED_COLUMNS]

#         # Drop rows that are entirely blank
#         df = df.dropna(how="all")

#         # Filter invalid StationIDs
#         valid_mask = df.apply(is_valid_record, axis=1)
#         invalid_rows = df[~valid_mask]
#         if not invalid_rows.empty:
#             failed_records = invalid_rows.to_dict(orient="records")
#             df = df[valid_mask]
#             bad_examples = [r["StationID"] for r in failed_records[:5]]
#             logging.warning(f"{file_name}: {len(invalid_rows)} invalid StationIDs skipped, examples: {bad_examples}")

#         if df.empty:
#             logging.warning(f"{file_name}: All records invalid or empty, skipped entirely.")
#             return

#         # Add UUID
#         df["uuid"] = df.apply(generate_uuid, axis=1)

#         # Insert
#         with connect_db() as conn:
#             with conn.cursor() as cur:
#                 cols = ", ".join([f'"{c}"' for c in df.columns])
#                 placeholders = ", ".join(["%s"] * len(df.columns))
#                 insert_sql = f"""
#                     INSERT INTO {TABLE_NAME} ({cols})
#                     VALUES ({placeholders})
#                     ON CONFLICT (uuid) DO NOTHING
#                 """
#                 execute_batch(cur, insert_sql, df.values.tolist(), page_size=500)

#                 # Audit
#                 cur.execute(f"""
#                     INSERT INTO {AUDIT_TABLE} 
#                     (file_name, record_count, success_count, fail_count, failed_records)
#                     VALUES (%s, %s, %s, %s, %s)
#                 """, (
#                     file_name,
#                     len(df) + len(failed_records),
#                     len(df),
#                     len(failed_records),
#                     failed_records if failed_records else None,
#                 ))
#             conn.commit()

#         print(f"✅ {file_name}: {len(df)} inserted, {len(failed_records)} invalid skipped.")
#         if failed_records:
#             logging.info(f"{file_name}: {len(df)} inserted, {len(failed_records)} invalid.")

#     except Exception as e:
#         logging.error(f"❌ {file_name}: {str(e)}")
#         print(f"❌ Failed for {file_name}: {e}")


# # ===============================================================
# # INGEST ALL
# # ===============================================================
# def ingest_all_csv(folder_path):
#     ensure_tables()
#     for file in os.listdir(folder_path):
#         if file.lower().endswith(".csv"):
#             ingest_csv(os.path.join(folder_path, file))


# # ===============================================================
# # MAIN
# # ===============================================================
# if __name__ == "__main__":
#     ingest_all_csv(CSV_FOLDER)

#========================================================================================================================================================================================

# import os
# import uuid
# import psycopg2
# import pandas as pd
# import logging
# import re
# import json
# from datetime import datetime
# from psycopg2.extras import execute_batch
# from dotenv import load_dotenv

# # ===============================================================
# # CONFIG
# # ===============================================================
# load_dotenv()
# DB_CONFIG = {
#     "host": os.getenv("DB_HOST"),
#     "port": os.getenv("DB_PORT"),
#     "dbname": os.getenv("DB_NAME"),
#     "user": os.getenv("DB_USER"),
#     "password": os.getenv("DB_PASSWORD"),
# }

# CSV_FOLDER = r"D:\FTP_SYNC_DATA\NHP_FTP_SYNC"
# #CSV_FOLDER = r"C:\Users\hp\Desktop\New folder"
# TABLE_NAME = "nhp_rtdas_ingest"
# AUDIT_TABLE = "nhp_rtdas_ingest_audit"

# EXPECTED_COLUMNS = [
#     "StationID", "DateTime", "MobileNumber", "Battery", "WaterLevel",
#     "HourlyRain", "DailyRain", "AT", "SnowDepth",
#     "Evaporation", "WS", "WD", "At.pressure", "RH", "Sun Radiation"
# ]

# # ===============================================================
# # LOGGING
# # ===============================================================
# LOG_FILE = "rtdas_ingest.log"
# logging.basicConfig(
#     filename=LOG_FILE,
#     level=logging.INFO,
#     format="%(asctime)s | %(levelname)s | %(message)s",
# )

# # ===============================================================
# # DB CONNECTION
# # ===============================================================
# def connect_db():
#     return psycopg2.connect(**DB_CONFIG)

# # ===============================================================
# # HELPERS
# # ===============================================================
# def generate_uuid(row):
#     row_str = "|".join(map(str, row.values))
#     return str(uuid.uuid5(uuid.NAMESPACE_DNS, row_str))


# # ------------------ Tokenizing-safe CSV reader ------------------
# def safe_read_csv(file_path):
#     """Reads CSV skipping only malformed rows + cleans NUL bytes."""
#     bad_rows = []

#     # Clean NUL bytes which break pandas
#     cleaned_lines = []
#     with open(file_path, "rb") as f:
#         for raw in f:
#             cleaned = raw.replace(b"\x00", b"")
#             try:
#                 cleaned_lines.append(cleaned.decode("utf-8", errors="ignore"))
#             except:
#                 continue

#     # Save to temp memory buffer
#     from io import StringIO
#     buffer = StringIO("".join(cleaned_lines))

#     # Collector for bad lines
#     def bad_line_collector(line):
#         bad_rows.append(line)
#         return None

#     df = pd.read_csv(
#         buffer,
#         dtype=str,
#         header=None,
#         engine="python",
#         on_bad_lines=bad_line_collector
#     )

#     return df, bad_rows


# def looks_like_date(value: str) -> bool:
#     if not value:
#         return False
#     s = value.strip()

#     patterns = [
#         r"\d{4}[-/]\d{2}[-/]\d{2}",
#         r"\d{2}[-/]\d{2}[-/]\d{4}",
#         r"\d{2}[-/]\d{2}[-/]\d{2}",
#         r"\d{8}",
#     ]
#     if any(re.fullmatch(p, s) for p in patterns):
#         return True

#     if ":" in s:
#         return True

#     return False


# def is_valid_record(row):
#     sid = str(row.get("StationID", "")).strip()
#     if not sid:
#         return False
#     if looks_like_date(sid):
#         return False
#     if sid.isdigit() and len(sid) < 6:
#         return False
#     return True


# def normalize_headers(cols):
#     mapping = {
#         "stationid": "StationID",
#         "station": "StationID",
#         "stid": "StationID",
#         "stnid": "StationID",
#         "dateandtime": "DateTime",
#         "datetime": "DateTime",
#         "date_time": "DateTime",
#         "datetimestamp": "DateTime",
#         "date": "Date",
#         "time": "Time",
#         "mobilenumber": "MobileNumber",
#         "mobile": "MobileNumber",
#         "mob": "MobileNumber",
#         "battery": "Battery",
#         "batt": "Battery",
#         "batteryvolt": "Battery",
#         "batteryvoltage": "Battery",
#         "waterlevel": "WaterLevel",
#         "wl": "WaterLevel",
#         "hourlyrain": "HourlyRain",
#         "hourrain": "HourlyRain",
#         "dailyrain": "DailyRain",
#         "dailyrainfall": "DailyRain",
#         "rain24": "DailyRain",
#         "at": "AT",
#         "airtemp": "AT",
#         "temp": "AT",
#         "snowdepth": "SnowDepth",
#         "snow": "SnowDepth",
#         "evaporation": "Evaporation",
#         "evap": "Evaporation",
#         "ws": "WS",
#         "wd": "WD",
#         "atpressure": "At.pressure",
#         "pressure": "At.pressure",
#         "baro": "At.pressure",
#         "rh": "RH",
#         "humidity": "RH",
#         "sunradiation": "Sun Radiation",
#         "solar": "Sun Radiation",
#         "radiation": "Sun Radiation",
#     }

#     normalized = []
#     for c in cols:
#         if not isinstance(c, str):
#             normalized.append(c)
#             continue
#         name = c.strip().replace("&", "").replace(" ", "").replace("_", "").lower()
#         normalized.append(mapping.get(name, c.strip()))
#     return normalized


# # ===============================================================
# # TABLE CREATION
# # ===============================================================
# def ensure_tables():
#     with connect_db() as conn:
#         with conn.cursor() as cur:
#             cols = ", ".join([f'"{c}" TEXT' for c in EXPECTED_COLUMNS + ["uuid"]])
#             cur.execute(f"""
#                 CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
#                     {cols},
#                     PRIMARY KEY (uuid)
#                 );
#             """)

#             cur.execute(f"""
#                 CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
#                     id SERIAL PRIMARY KEY,
#                     file_name TEXT,
#                     record_count INT,
#                     success_count INT,
#                     fail_count INT,
#                     failed_records JSONB,
#                     timestamp TIMESTAMP DEFAULT NOW()
#                 );
#             """)
#         conn.commit()


# # ===============================================================
# # INGEST FUNCTION
# # ===============================================================
# def ingest_csv(file_path):
#     file_name = os.path.basename(file_path)
#     failed_records = []
#     print(f"Processing: {file_name}")

#     try:
#         # Safe read (skip only bad lines)
#         df_raw, bad_rows = safe_read_csv(file_path)

#         # Log tokenizing errors
#         if bad_rows:
#             logging.error(f"{file_name}: {len(bad_rows)} tokenizing rows skipped.")
#             failed_records.extend([{"error": "tokenize", "raw": str(r)} for r in bad_rows])

#         # Detect header
#         first_row = df_raw.iloc[0].astype(str).str.lower().tolist()
#         has_header = any("station" in x or "date" in x for x in first_row)

#         if has_header:
#             df = df_raw[1:].copy()
#             df.columns = normalize_headers(first_row)
#         else:
#             df = df_raw.copy()
#             df.columns = EXPECTED_COLUMNS[:len(df.columns)]

#         df.columns = normalize_headers(df.columns)

#         # Merge Date + Time
#         if "Date" in df.columns and "Time" in df.columns:
#             df["DateTime"] = df["Date"].astype(str).str.strip() + " " + df["Time"].astype(str).str.strip()
#             df = df.drop(columns=["Date", "Time"])

#         # Fill missing columns
#         for col in EXPECTED_COLUMNS:
#             if col not in df.columns:
#                 df[col] = None

#         df = df[EXPECTED_COLUMNS]
#         df = df.dropna(how="all")

#         # StationID filtering
#         invalid_rows = df[~df.apply(is_valid_record, axis=1)]

#         if not invalid_rows.empty:
#             failed_records.extend(invalid_rows.to_dict(orient="records"))
#             df = df[df.apply(is_valid_record, axis=1)]

#         if df.empty:
#             logging.error(f"{file_name}: all rows invalid.")
#             return

#         df["uuid"] = df.apply(generate_uuid, axis=1)

#         # Insert OK rows
#         with connect_db() as conn:
#             with conn.cursor() as cur:
#                 cols = ", ".join([f'"{c}"' for c in df.columns])
#                 ph = ", ".join(["%s"] * len(df.columns))

#                 execute_batch(cur,
#                     f"INSERT INTO {TABLE_NAME} ({cols}) VALUES ({ph}) ON CONFLICT (uuid) DO NOTHING",
#                     df.values.tolist(),
#                     page_size=500
#                 )

#                 cur.execute(f"""
#                     INSERT INTO {AUDIT_TABLE}
#                     (file_name, record_count, success_count, fail_count, failed_records)
#                     VALUES (%s, %s, %s, %s, %s)
#                 """, (
#                     file_name,
#                     len(df) + len(failed_records),
#                     len(df),
#                     len(failed_records),
#                     json.dumps(failed_records) if failed_records else None,
#                 ))

#             conn.commit()

#         print(f"✅ {file_name}: {len(df)} inserted, {len(failed_records)} skipped.")

#     except Exception as e:
#         logging.error(f"{file_name}: {str(e)}")
#         print(f"❌ Failed for {file_name}: {e}")


# # ===============================================================
# # RUN ALL
# # ===============================================================
# def ingest_all_csv(folder_path):
#     ensure_tables()
#     for file in os.listdir(folder_path):
#         if file.lower().endswith(".csv"):
#             ingest_csv(os.path.join(folder_path, file))


# if __name__ == "__main__":
#     ingest_all_csv(CSV_FOLDER)

#=========================================================================================================================================================================================

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
PROCESSED_TABLE = "nhp_rtdas_processed_files"

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

# Also log errors to console for quick feedback
console = logging.StreamHandler()
console.setLevel(logging.ERROR)
logging.getLogger().addHandler(console)

# ===============================================================
# DB CONNECTION
# ===============================================================
def connect_db():
    return psycopg2.connect(**DB_CONFIG)

# ===============================================================
# HELPERS
# ===============================================================
def generate_uuid(row):
    row_str = "|".join(map(str, row.values))
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, row_str))


# ------------------ Tokenizing-safe CSV reader ------------------
def safe_read_csv(file_path):
    """Reads CSV skipping only malformed rows + cleans NUL bytes.
    Returns (df, bad_rows_list). df is read with header=None.
    """
    bad_rows = []

    # Clean NUL bytes which break pandas and collect cleaned text lines
    cleaned_lines = []
    with open(file_path, "rb") as f:
        for raw in f:
            cleaned = raw.replace(b"\x00", b"")
            try:
                cleaned_lines.append(cleaned.decode("utf-8", errors="ignore"))
            except Exception:
                # skip lines that can't decode
                continue

    # Save to temp memory buffer
    from io import StringIO
    buffer = StringIO("".join(cleaned_lines))

    # Collector for bad lines
    def bad_line_collector(line):
        bad_rows.append(line)
        return None

    # Use python engine to allow on_bad_lines callable
    df = pd.read_csv(
        buffer,
        dtype=str,
        header=None,
        engine="python",
        on_bad_lines=bad_line_collector
    )

    return df, bad_rows


def looks_like_date(value: str) -> bool:
    if not value:
        return False
    s = value.strip()

    patterns = [
        r"\d{4}[-/]\d{2}[-/]\d{2}",
        r"\d{2}[-/]\d{2}[-/]\d{4}",
        r"\d{2}[-/]\d{2}[-/]\d{2}",
        r"\d{8}",
    ]
    # fullmatch for pure date tokens, otherwise allow colon or timestamps
    if any(re.fullmatch(p, s) for p in patterns):
        return True

    # If contains time separator or likely datetime token
    if ":" in s or re.search(r"\d{2}\s+\d{2}", s):
        return True

    return False


def is_valid_record(row):
    sid = str(row.get("StationID", "")).strip()
    if not sid:
        return False
    if looks_like_date(sid):
        return False
    if sid.isdigit() and len(sid) < 6:
        return False
    return True


def normalize_headers(cols):
    mapping = {
        "stationid": "StationID",
        "station": "StationID",
        "stid": "StationID",
        "stnid": "StationID",
        "dateandtime": "DateTime",
        "datetime": "DateTime",
        "date_time": "DateTime",
        "datetimestamp": "DateTime",
        "date": "Date",
        "time": "Time",
        "mobilenumber": "MobileNumber",
        "mobile": "MobileNumber",
        "mob": "MobileNumber",
        "battery": "Battery",
        "batt": "Battery",
        "batteryvolt": "Battery",
        "batteryvoltage": "Battery",
        "waterlevel": "WaterLevel",
        "wl": "WaterLevel",
        "hourlyrain": "HourlyRain",
        "hourrain": "HourlyRain",
        "dailyrain": "DailyRain",
        "dailyrainfall": "DailyRain",
        "rain24": "DailyRain",
        "at": "AT",
        "airtemp": "AT",
        "temp": "AT",
        "snowdepth": "SnowDepth",
        "snow": "SnowDepth",
        "evaporation": "Evaporation",
        "evap": "Evaporation",
        "ws": "WS",
        "wd": "WD",
        "atpressure": "At.pressure",
        "pressure": "At.pressure",
        "baro": "At.pressure",
        "rh": "RH",
        "humidity": "RH",
        "sunradiation": "Sun Radiation",
        "solar": "Sun Radiation",
        "radiation": "Sun Radiation",
    }

    normalized = []
    for c in cols:
        if not isinstance(c, str):
            normalized.append(c)
            continue
        name = c.strip().replace("&", "").replace(" ", "").replace("_", "").lower()
        normalized.append(mapping.get(name, c.strip()))
    return normalized


# ===============================================================
# TABLE CREATION (including processed files table)
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

            # table to record processed files
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {PROCESSED_TABLE} (
                    file_name TEXT PRIMARY KEY,
                    processed_at TIMESTAMP DEFAULT NOW()
                );
            """)
        conn.commit()


# ===============================================================
# processed-files helpers
# ===============================================================
def is_already_processed(file_name: str) -> bool:
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT 1 FROM {PROCESSED_TABLE} WHERE file_name = %s", (file_name,))
            return cur.fetchone() is not None


def mark_processed(file_name: str):
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {PROCESSED_TABLE} (file_name) VALUES (%s) ON CONFLICT (file_name) DO UPDATE SET processed_at = NOW()",
                (file_name,)
            )
        conn.commit()


# ===============================================================
# INGEST FUNCTION (per-file)
# ===============================================================
def ingest_csv(file_path):
    file_name = os.path.basename(file_path)
    failed_records = []
    print(f"Processing: {file_name}")

    # Skip quickly if DB already has this file processed
    try:
        if is_already_processed(file_name):
            print(f"⏭ {file_name} already processed — skipping.")
            return
    except Exception as e:
        logging.error(f"{file_name}: error checking processed table: {e}")
        # proceed — we don't want to block ingestion if processed check fails

    try:
        # Safe read (skip only malformed rows)
        df_raw, bad_rows = safe_read_csv(file_path)

        # Record tokenizing/bad-line errors (these are per-row skips, not full-file)
        if bad_rows:
            logging.error(f"{file_name}: {len(bad_rows)} tokenizing rows skipped.")
            failed_records.extend([{"error": "tokenize", "raw": str(r)} for r in bad_rows])

        # Header detection
        first_row = df_raw.iloc[0].astype(str).str.lower().tolist() if not df_raw.empty else []
        has_header = any("station" in x or "date" in x for x in first_row) if first_row else False

        if has_header:
            # treat the first row as header values, data starts from second row
            data_df = df_raw[1:].copy()
            # normalize the header labels from first_row
            header_labels = normalize_headers([str(x) for x in df_raw.iloc[0].tolist()])
            data_df.columns = header_labels
        else:
            data_df = df_raw.copy()
            # assign fixed header positions up to number of columns
            data_df.columns = EXPECTED_COLUMNS[:len(data_df.columns)]

        # normalize column names
        data_df.columns = normalize_headers(data_df.columns)

        # Merge Date + Time into DateTime if present
        if "Date" in data_df.columns and "Time" in data_df.columns:
            data_df["DateTime"] = data_df["Date"].astype(str).str.strip() + " " + data_df["Time"].astype(str).str.strip()
            data_df = data_df.drop(columns=["Date", "Time"])

        # Ensure expected columns exist; fill missing with None
        for col in EXPECTED_COLUMNS:
            if col not in data_df.columns:
                data_df[col] = None

        # Trim/align to EXACT expected order
        df = data_df[EXPECTED_COLUMNS].copy()

        # Drop rows that are entirely blank
        df = df.dropna(how="all")

        # StationID filtering — collect invalid rows and keep the valids
        valid_mask = df.apply(is_valid_record, axis=1)
        invalid_rows = df[~valid_mask]
        if not invalid_rows.empty:
            # extend failed_records with a small subset of columns to reduce payload
            failed_records.extend(invalid_rows.head(200).to_dict(orient="records"))
            df = df[valid_mask]
            bad_examples = [r.get("StationID") for r in (invalid_rows.head(5).to_dict(orient="records"))]
            logging.warning(f"{file_name}: {len(invalid_rows)} invalid StationIDs skipped, examples: {bad_examples}")

        if df.empty:
            logging.error(f"{file_name}: all rows invalid or empty. Skipping file.")
            # still write an audit record to show it was processed (optional). We'll write audit with zero success.
            try:
                with connect_db() as conn:
                    with conn.cursor() as cur:
                        cur.execute(f"""
                            INSERT INTO {AUDIT_TABLE} (file_name, record_count, success_count, fail_count, failed_records)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (
                            file_name,
                            len(invalid_rows),
                            0,
                            len(invalid_rows),
                            json.dumps(failed_records) if failed_records else None
                        ))
                    conn.commit()
            except Exception as e:
                logging.error(f"{file_name}: couldn't write audit record for all-invalid file: {e}")
            # do not mark processed when all invalid? Decide: mark as processed to avoid re-processing.
            try:
                mark_processed(file_name)
            except Exception as e:
                logging.error(f"{file_name}: couldn't mark processed: {e}")
            return

        # Add UUID
        df["uuid"] = df.apply(generate_uuid, axis=1)

        # Insert valid rows and audit
        with connect_db() as conn:
            with conn.cursor() as cur:
                cols = ", ".join([f'"{c}"' for c in df.columns])
                ph = ", ".join(["%s"] * len(df.columns))

                # Bulk insert using execute_batch
                execute_batch(
                    cur,
                    f"INSERT INTO {TABLE_NAME} ({cols}) VALUES ({ph}) ON CONFLICT (uuid) DO NOTHING",
                    df.values.tolist(),
                    page_size=500
                )

                # audit (store failed_records as JSON)
                cur.execute(f"""
                    INSERT INTO {AUDIT_TABLE}
                    (file_name, record_count, success_count, fail_count, failed_records)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    file_name,
                    len(df) + len(failed_records),
                    len(df),
                    len(failed_records),
                    json.dumps(failed_records) if failed_records else None,
                ))
            conn.commit()

        # mark file as processed only after successful commit
        try:
            mark_processed(file_name)
        except Exception as e:
            logging.error(f"{file_name}: couldn't mark processed: {e}")

        print(f"✅ {file_name}: {len(df)} inserted, {len(failed_records)} skipped.")
        if failed_records:
            logging.info(f"{file_name}: {len(df)} inserted, {len(failed_records)} invalid.")

    except Exception as e:
        logging.error(f"{file_name}: {str(e)}")
        print(f"❌ Failed for {file_name}: {e}")


# ===============================================================
# RUN ALL (with multiprocessing and processed-file skipping)
# ===============================================================
def ingest_all_csv(folder_path, use_multiprocessing=True, max_workers=None):
    ensure_tables()

    # gather candidate files
    all_csvs = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.lower().endswith(".csv")
    ]

    # filter out already processed files (do this in main process)
    to_process = []
    try:
        with connect_db() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT file_name FROM {PROCESSED_TABLE}")
                processed = {r[0] for r in cur.fetchall()}
        for p in all_csvs:
            fname = os.path.basename(p)
            if fname not in processed:
                to_process.append(p)
    except Exception as e:
        # If processed-files check fails, fall back to processing all files
        logging.error(f"Error reading processed files table, proceeding with all files: {e}")
        to_process = all_csvs

    if not to_process:
        print("No new CSV files to process.")
        return

    # decide workers
    cpu = cpu_count()
    if max_workers is None:
        # choose a safe parallelism level
        num_workers = min(4, max(1, cpu // 2))
    else:
        num_workers = max(1, min(max_workers, cpu))

    if use_multiprocessing and num_workers > 1:
        print(f"🚀 Processing {len(to_process)} files with {num_workers} workers...")
        with Pool(processes=num_workers) as pool:
            pool.map(ingest_csv, to_process)
    else:
        print(f"⚙️ Processing {len(to_process)} files serially...")
        for p in to_process:
            ingest_csv(p)


# ===============================================================
# MAIN
# ===============================================================
if __name__ == "__main__":
    ingest_all_csv(CSV_FOLDER)


