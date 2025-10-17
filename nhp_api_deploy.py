from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from typing import List, Optional
import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text

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

app = FastAPI(title="NHP RTDAS API", version="1.0")

def get_current_user(credentials: HTTPBasicCredentials = Depends(security)):
    if credentials.username != USERNAME or credentials.password != PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return credentials.username

# === SQLAlchemy engine ===
engine_url = (
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)
engine = create_engine(engine_url)


# ------------------ Helper: case-insensitive LIKE pattern ------------------
def make_pattern(value: str) -> str:
    """
    Normalize and create a fuzzy search pattern.
    Converts 'kamrup' -> '%kamrup%'
    Removes special chars for more flexible matching.
    """
    value = value.strip().lower()
    #value = value.replace("-", "").replace("(", "").replace(")", "")
    for ch in ['-', '(', ')', ',']:
        value = value.replace(ch, '')
    return f"%{value}%"


#============Main data endpoint=================

@app.get("/stations/data")
def get_station_data(
    start_date: Optional[str] = Query(None, description="Filter by start date (YYYY-MM-DD, DD-MM-YYYY)"),
    end_date: Optional[str] = Query(None, description="Filter by end date (YYYY-MM-DD, DD-MM-YYYY)"),
    station_type: Optional[str] = Query(None, description="Filter by station type (AWLR, ARG, AWS, ARG+AWLR)"),
    zone: Optional[str] = Query(None, description="Filter by zone name"),
    location: Optional[str] = Query(None, description="Filter by location name"),
    district: Optional[str] = Query(None, description="Filter by district name"),
    page: Optional[int] = Query(None, ge=1, description="Page number (default 1)"),
    page_size: Optional[int] = Query(None, ge=1, le=500, description="Records per page (default 50)"),
    user: str = Depends(get_current_user),
):
    """
    Fetch RTDAS + master station data with optional filters:
    - Date range (start_date, end_date)
    - Station_type/Zone/location/District
    - Pagination
    """

    # === Default pagination ===
    page = page or 1
    page_size = page_size or 50

    # === Base query ===
    base_query = """
        FROM nhp_v2 m
        JOIN nhp_rtdas_ingest d ON m.id = d."StationID"
        WHERE 1=1
    """

    filters = []
    params = {}

    # === Date range filters ===
    if start_date and end_date:
        filters.append('AND d."DateTime"::date BETWEEN :start_date AND :end_date')
        params["start_date"] = start_date
        params["end_date"] = end_date
    elif start_date:
        filters.append('AND d."DateTime"::date >= :start_date')
        params["start_date"] = start_date
    elif end_date:
        filters.append('AND d."DateTime"::date <= :end_date')
        params["end_date"] = end_date

    # === Additional filters ===
  
    if district:
        filters.append("AND LOWER(m.district) ILIKE :district")
        params["district"] = make_pattern(district)
    if location:
        filters.append("AND LOWER(m.location) ILIKE :location")
        params["location"] = make_pattern(location)
    if zone:
        filters.append("AND LOWER(m.zone) ILIKE :zone")
        params["zone"] = make_pattern(zone)
    if station_type:
        filters.append("AND LOWER(m.type) ILIKE :station_type")
        params["station_type"] = make_pattern(station_type)
    

    # === Data query with pagination ===
    data_query = text(f"""
        SELECT
            m.id AS station_id,
            m.longitude,
            m.latitude,
            m.zone,
            m.name,
            m.type,
            m.location,
            m.district,                    
            d."MobileNumber",
            d."Battery",
            d."WaterLevel",
            d."HourlyRain",
            d."DailyRainfall",
            d."DateTime"
        {base_query}
        {' '.join(filters)}
        ORDER BY d."DateTime" DESC
        LIMIT :limit OFFSET :offset
    """)

    params["limit"] = page_size
    params["offset"] = (page - 1) * page_size

    # === Count query ===
    count_query = text(f"""
        SELECT COUNT(*) AS total
        {base_query}
        {' '.join(filters)}
    """)

    with engine.connect() as conn:
        df = pd.read_sql(data_query, conn, params=params)
        total_records = conn.execute(count_query, params).scalar() or 0

    return {
        "page": page,
        "page_size": page_size,
        "total_records": int(total_records),
        "total_pages": (int(total_records) + page_size - 1) // page_size,
        "data": df.to_dict(orient="records"),
    }

#===============Latest record endpoint==========================


@app.get("/stations/latest")
def get_latest_station_data(
    station_type: Optional[str] = Query(None, description="Filter by station type"),
    zone: Optional[str] = Query(None, description="Filter by zone name"),
    district: Optional[str] = Query(None, description="Filter by district name"),
    location: Optional[str] = Query(None, description="Filter by location name"),
    limit: Optional[int] = Query(1, ge=1, le=20, description="Number of latest records per station (default 1, max 20)"),
    user: str = Depends(get_current_user),
):
    """
    Fetch latest N records per station (default = 1, max = 20).
    Optional fuzzy normalized filters: station_type, zone, district, location.
    """

    params = {"limit": limit}
    meta_filters = []

    # Filters on master table
    
    if district:
        meta_filters.append("AND LOWER(m.district) ILIKE :district")
        params["district"] = make_pattern(district)
    if location:
        meta_filters.append("AND LOWER(m.location) ILIKE :location")
        params["location"] = make_pattern(location)
    if zone:
        meta_filters.append("AND LOWER(m.zone) ILIKE :zone")
        params["zone"] = make_pattern(zone)
    if station_type:
        meta_filters.append("AND LOWER(m.type) ILIKE :station_type")
        params["station_type"] = make_pattern(station_type)

    meta_where = " ".join(meta_filters)

    query = text(f"""
        WITH ranked AS (
            SELECT
                d."StationID",
                d."DateTime",
                d."MobileNumber",
                d."Battery",
                d."WaterLevel",
                d."HourlyRain",
                d."DailyRainfall",
                ROW_NUMBER() OVER (PARTITION BY d."StationID" ORDER BY d."DateTime"::timestamp DESC) AS rn
            FROM nhp_rtdas_ingest d
        )
        SELECT
            m.id AS station_id,
            m.longitude,
            m.latitude,
            m.zone,
            m.name,
            m.type,
            m.location,
            m.district,          
            r."MobileNumber",
            r."Battery",
            r."WaterLevel",
            r."HourlyRain",
            r."DailyRainfall",
            r."DateTime"
        FROM ranked r
        JOIN nhp_v2 m ON m.id = r."StationID"
        WHERE r.rn <= :limit {meta_where}
        ORDER BY r."StationID", r."DateTime" DESC;
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)

    return {
        "limit_per_station": limit,
        "total_records": len(df),
        "data": df.to_dict(orient="records"),
    }


#=====================Station Meta data endpoint=================================================================================

@app.get("/master/filter")
def get_filtered(
    district: Optional[str] = Query(None, description="District name (case-insensitive)"),
    location: Optional[str] = Query(None, description="Location name (case-insensitive)"),
    zone: Optional[str] = Query(None, description="Zone name (case-insensitive)"),
    station_type: Optional[str] = Query(None, description="Sensor type (case-insensitive)"),
    user: str = Depends(get_current_user)
):
    """
    Fetch meta data.
    Optional fuzzy normalized filters: station_type, zone, district, location.
    """

    filters = []
    params = {}

    if district:
        filters.append("AND LOWER(district) ILIKE :district")
        params["district"] = make_pattern(district)
    if location:
        filters.append("AND LOWER(location) ILIKE :location")
        params["location"] = make_pattern(location)
    if zone:
        filters.append("AND LOWER(zone) ILIKE :zone")
        params["zone"] = make_pattern(zone)
    if station_type:
        filters.append("AND LOWER(type) ILIKE :station_type")
        params["station_type"] = make_pattern(station_type)


    query = text(f"""
        SELECT id, district, name, location, zone, latitude, longitude, type
        FROM nhp_v2
        WHERE 1=1 {' '.join(filters)}
        ORDER BY "id";
    """)
    
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params=params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    return {
        "total_records": len(df),
        "meta data": df.to_dict(orient="records"),
    }
    