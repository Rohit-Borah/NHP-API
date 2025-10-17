from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os
from typing import Optional

# ------------------ Load Environment ------------------
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

USERNAME = os.getenv("API_USER")
PASSWORD = os.getenv("API_PASS")

# ------------------ FastAPI App ------------------
app = FastAPI(title="NHP Master Metadata API", version="1.2")

security = HTTPBasic()

# ------------------ Auth ------------------
def get_current_user(credentials: HTTPBasicCredentials = Depends(security)):
    if credentials.username != USERNAME or credentials.password != PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return credentials.username

# ------------------ DB Connection ------------------
def get_connection():
    return psycopg2.connect(**DB_CONFIG)

# ------------------ Helper: case-insensitive LIKE pattern ------------------
def make_pattern(value: str) -> str:
    """
    Normalize and create a fuzzy search pattern.
    Converts 'kamrup' -> '%kamrup%'
    Removes special chars for more flexible matching.
    """
    value = value.strip().lower()
    value = value.replace("-", "").replace("(", "").replace(")", "")
    return f"%{value}%"

# ------------------ Core Query Function ------------------
def fetch_master_data(filter_clause: str = "", params: list = []):
    query = f"""
        SELECT gid, id, district, name, location, zone, latitude, longitude, type
        FROM nhp_v2
        WHERE 1=1 {filter_clause}
        ORDER BY id;
    """
    try:
        with get_connection() as conn:
            df = pd.read_sql(query, conn, params=params)
        return df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

# ------------------ Routes ------------------

# @app.get("/master/all")
# def get_all_master_data(user: str = Depends(get_current_user)):
#     """Fetch all records from NHP master table"""
#     return fetch_master_data()

# @app.get("/master/by_district")
# def get_by_district(
#     district: str = Query(..., description="District name to filter (case-insensitive)"),
#     user: str = Depends(get_current_user)
# ):
#     pattern = make_pattern(district)
#     return fetch_master_data("AND LOWER(REPLACE(REPLACE(REPLACE(district, '-', ''), '(', ''), ')', '')) ILIKE %s", [pattern])

# @app.get("/master/by_location")
# def get_by_location(
#     location: str = Query(..., description="Location name to filter (case-insensitive)"),
#     user: str = Depends(get_current_user)
# ):
#     pattern = make_pattern(location)
#     return fetch_master_data("AND LOWER(location) ILIKE %s", [pattern])

# @app.get("/master/by_zone")
# def get_by_zone(
#     zone: str = Query(..., description="Zone name to filter (case-insensitive)"),
#     user: str = Depends(get_current_user)
# ):
#     pattern = make_pattern(zone)
#     return fetch_master_data("AND LOWER(zone) ILIKE %s", [pattern])

# @app.get("/master/by_type")
# def get_by_type(
#     type: str = Query(..., description="Type to filter (case-insensitive)"),
#     user: str = Depends(get_current_user)
# ):
#     pattern = make_pattern(type)
#     return fetch_master_data("AND LOWER(type) ILIKE %s", [pattern])

@app.get("/master/filter")
def get_filtered(
    district: Optional[str] = Query(None, description="District name (case-insensitive)"),
    location: Optional[str] = Query(None, description="Location name (case-insensitive)"),
    zone: Optional[str] = Query(None, description="Zone name (case-insensitive)"),
    type: Optional[str] = Query(None, description="Sensor type (case-insensitive)"),
    user: str = Depends(get_current_user)
):
    filters, params = [], []
    if district:
        filters.append("AND LOWER(district) ILIKE %s")
        params.append(make_pattern(district))
    if location:
        filters.append("AND LOWER(location) ILIKE %s")
        params.append(make_pattern(location))
    if zone:
        filters.append("AND LOWER(zone) ILIKE %s")
        params.append(make_pattern(zone))
    if type:
        filters.append("AND LOWER(type) ILIKE %s")
        params.append(make_pattern(type))

    df = fetch_master_data(" ".join(filters), params)
    
    return {
        "count": len(df),
        "records": df.to_dict(orient="records")
    }
