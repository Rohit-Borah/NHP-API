from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from typing import List, Optional
import psycopg2
import os
from dotenv import load_dotenv
import pandas as pd

# Load env
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# Basic Auth
security = HTTPBasic()
USERNAME = os.getenv("API_USER")
PASSWORD = os.getenv("API_PASS")

app = FastAPI(title="RTDAS API", version="1.0")

def get_current_user(credentials: HTTPBasicCredentials = Depends(security)):
    if credentials.username != USERNAME or credentials.password != PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return credentials.username

def get_connection():
    return psycopg2.connect(**DB_CONFIG)


@app.get("/stations/data")
def get_station_data(
    date: Optional[str] = None,
    station_type: Optional[str] = None,
    basin: Optional[str] = None,
    user: str = Depends(get_current_user)
):
    """
    Returns station + RTDAS data
    Filters: date (YYYY-MM-DD), station_type (AWLR, AWLR_ARG), basin (Beki, Jiadhal, Buridehing)
    """

    query = f"""
        SELECT
            m.id AS station_id,
            m.longitude,
            m.latitude,
            m.basin,
            m.name,
            m.type,
            d."MobileNumber",
            d."Battery",
            d."WaterLevel",
            d."HourlyRain",
            d."DailyRainfall"
        FROM nhp_rtdas_master m
        JOIN nhp_rtdas_ingest d ON m.id = d."StationID"
        WHERE 1=1
    """

    params = []
    if date:
        query += ' AND d."DateTime"::date = %s'
        params.append(date)
    if station_type:
        query += " AND m.type = %s"
        params.append(station_type)
    if basin:
        query += " AND m.basin = %s"
        params.append(basin)

    with get_connection() as conn:
        df = pd.read_sql(query, conn, params=params)

    return df.to_dict(orient="records")
