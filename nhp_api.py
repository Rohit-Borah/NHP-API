from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from typing import List, Optional
import psycopg2
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


#--------------------BASIC-----------------------------------------------------------------------------------------------

# def get_connection():
#     return psycopg2.connect(**DB_CONFIG)

# @app.get("/stations/data")
# def get_station_data(
#     date: Optional[str] = None,
#     station_type: Optional[str] = None,
#     basin: Optional[str] = None,
#     user: str = Depends(get_current_user)
# ):
#     """
#     Returns station + RTDAS data
#     Filters: date (YYYY-MM-DD), station_type (AWLR, AWLR_ARG), basin (Beki, Jiadhal, Buridehing)
#     """

#     query = f"""
#         SELECT
#             m.id AS station_id,
#             m.longitude,
#             m.latitude,
#             m.basin,
#             m.name,
#             m.type,
#             d."MobileNumber",
#             d."Battery",
#             d."WaterLevel",
#             d."HourlyRain",
#             d."DailyRainfall"
#         FROM nhp_rtdas_master m
#         JOIN nhp_rtdas_ingest d ON m.id = d."StationID"
#         WHERE 1=1
#     """

#     params = []
#     if date:
#         query += ' AND d."DateTime"::date = %s'
#         params.append(date)
#     if station_type:
#         query += " AND m.type = %s"
#         params.append(station_type)
#     if basin:
#         query += " AND m.basin = %s"
#         params.append(basin)

#     with get_connection() as conn:
#         df = pd.read_sql(query, conn, params=params)

#     return df.to_dict(orient="records")

#---------------------WITH PAGINATION--------------------------------------------------------------------------------------------------------------------------------------------

# def get_connection():
#     return psycopg2.connect(**DB_CONFIG)

# @app.get("/stations/data")
# def get_station_data(
#     date: Optional[str] = Query(None, description="Filter by date (YYYY-MM-DD)"),
#     station_type: Optional[str] = Query(None, description="Filter by station type"),
#     basin: Optional[str] = Query(None, description="Filter by basin name"),
#     page: Optional[int] = Query(None, ge=1, description="Page number (default 1)"),
#     page_size: Optional[int] = Query(None, ge=1, le=500, description="Records per page (default 50)"),
#     user: str = Depends(get_current_user)
# ):
#     """
#     Fetch paginated RTDAS + master station data.
#     Filters: date, station_type, basin
#     Pagination: optional (?page=2&page_size=100)
#     """

#     # === Default pagination if not provided ===
#     page = page or 1
#     page_size = page_size or 50

#     query = f"""
#         SELECT
#             m.id AS station_id,
#             m.longitude,
#             m.latitude,
#             m.basin,
#             m.name,
#             m.type,
#             d."MobileNumber",
#             d."Battery",
#             d."WaterLevel",
#             d."HourlyRain",
#             d."DailyRainfall",
#             d."DateTime"
#         FROM nhp_rtdas_master m
#         JOIN nhp_rtdas_ingest d ON m.id = d."StationID"
#         WHERE 1=1
#     """

#     params = []
#     if date:
#         query += ' AND d."DateTime"::date = %s'
#         params.append(date)
#     if station_type:
#         query += " AND m.type = %s"
#         params.append(station_type)
#     if basin:
#         query += " AND m.basin = %s"
#         params.append(basin)

#     # Order + pagination
#     query += ' ORDER BY d."DateTime" DESC LIMIT %s OFFSET %s'
#     params.extend([page_size, (page - 1) * page_size])

#     with get_connection() as conn:
#         df = pd.read_sql(query, conn, params=params)

#         # Count query for pagination metadata
#         count_query = f"""
#             SELECT COUNT(*) FROM nhp_rtdas_master m
#             JOIN nhp_rtdas_ingest d ON m.id = d."StationID"
#             WHERE 1=1
#         """
#         count_params = []
#         if date:
#             count_query += ' AND d."DateTime"::date = %s'
#             count_params.append(date)
#         if station_type:
#             count_query += " AND m.type = %s"
#             count_params.append(station_type)
#         if basin:
#             count_query += " AND m.basin = %s"
#             count_params.append(basin)

#         total_records = pd.read_sql(count_query, conn, params=count_params).iloc[0, 0]

#     return {
#         "page": page,
#         "page_size": page_size,
#         "total_records": int(total_records),
#         "total_pages": (int(total_records) + page_size - 1) // page_size,
#         "data": df.to_dict(orient="records")
#     }

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# # === SQLAlchemy engine ===
# engine_url = (
#     f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
#     f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
# )
# engine = create_engine(engine_url)

# @app.get("/stations/data")
# def get_station_data(
#     date: Optional[str] = Query(None, description="Filter by date (YYYY-MM-DD)"),
#     station_type: Optional[str] = Query(None, description="Filter by station type"),
#     basin: Optional[str] = Query(None, description="Filter by basin name"),
#     page: Optional[int] = Query(None, ge=1, description="Page number (default 1)"),
#     page_size: Optional[int] = Query(None, ge=1, le=500, description="Records per page (default 50)"),
#     user: str = Depends(get_current_user)
# ):
#     """Fetch paginated RTDAS + master station data."""

#     # Default pagination
#     page = page or 1
#     page_size = page_size or 50

#     # === Main query ===
#     base_query = """
#         FROM nhp_rtdas_master m
#         JOIN nhp_rtdas_ingest d ON m.id = d."StationID"
#         WHERE 1=1
#     """

#     filters = []
#     params = {}

#     if date:
#         filters.append('AND d."DateTime"::date = :date')
#         params["date"] = date
#     if station_type:
#         filters.append("AND m.type = :station_type")
#         params["station_type"] = station_type
#     if basin:
#         filters.append("AND m.basin = :basin")
#         params["basin"] = basin

#     # === Data query with pagination ===
#     data_query = text(f"""
#         SELECT
#             m.id AS station_id,
#             m.longitude,
#             m.latitude,
#             m.basin,
#             m.name,
#             m.type,
#             d."MobileNumber",
#             d."Battery",
#             d."WaterLevel",
#             d."HourlyRain",
#             d."DailyRainfall",
#             d."DateTime"
#         {base_query}
#         {' '.join(filters)}
#         ORDER BY d."DateTime" DESC
#         LIMIT :limit OFFSET :offset
#     """)

#     params["limit"] = page_size
#     params["offset"] = (page - 1) * page_size

#     # === Count query ===
#     count_query = text(f"""
#         SELECT COUNT(*) AS total
#         {base_query}
#         {' '.join(filters)}
#     """)

#     with engine.connect() as conn:
#         df = pd.read_sql(data_query, conn, params=params)
#         total_records = conn.execute(count_query, params).scalar() or 0

#     return {
#         "page": page,
#         "page_size": page_size,
#         "total_records": int(total_records),  # Fix numpy.int64 issue
#         "total_pages": (int(total_records) + page_size - 1) // page_size,
#         "data": df.to_dict(orient="records"),
#     }

#-------------------------------------------------------------------------------------------------------------------------------------------------------------------
# === SQLAlchemy engine ===
engine_url = (
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)
engine = create_engine(engine_url)

#============Main data endpoint=================

@app.get("/stations/data")
def get_station_data(
    start_date: Optional[str] = Query(None, description="Filter by start date (YYYY-MM-DD, DD-MM-YYYY)"),
    end_date: Optional[str] = Query(None, description="Filter by end date (YYYY-MM-DD, DD-MM-YYYY)"),
    station_type: Optional[str] = Query(None, description="Filter by station type (AWLR, AWLR_ARG)"),
    basin: Optional[str] = Query(None, description="Filter by basin name (Beki, Jiadhal, Buridehing)"),
    page: Optional[int] = Query(None, ge=1, description="Page number (default 1)"),
    page_size: Optional[int] = Query(None, ge=1, le=500, description="Records per page (default 50)"),
    user: str = Depends(get_current_user),
):
    """
    Fetch RTDAS + master station data with optional filters:
    - Date range (start_date, end_date)
    - Station_type/Basin
    - Pagination
    - Latest record per station
    """

    # === Default pagination ===
    page = page or 1
    page_size = page_size or 50

    # === Base query ===
    base_query = """
        FROM nhp_rtdas_master m
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
    if station_type:
        filters.append("AND m.type = :station_type")
        params["station_type"] = station_type
    if basin:
        filters.append("AND m.basin = :basin")
        params["basin"] = basin

    # === Data query with pagination ===
    data_query = text(f"""
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
    basin: Optional[str] = Query(None, description="Filter by basin name"),
    limit: Optional[int] = Query(1, ge=1, le=20, description="Number of latest records per station (default 1, max 20)"),
    user: str = Depends(get_current_user),
):
    """
    Fetch latest N records per station (default = 1, max = 20).
    Optional filters: station_type, basin.
    """

    params = {"limit": limit}
    meta_filters = []

    # Filters on master table
    if station_type:
        meta_filters.append("AND m.type = :station_type")
        params["station_type"] = station_type
    if basin:
        meta_filters.append("AND m.basin = :basin")
        params["basin"] = basin

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
            m.basin,
            m.name,
            m.type,
            r."MobileNumber",
            r."Battery",
            r."WaterLevel",
            r."HourlyRain",
            r."DailyRainfall",
            r."DateTime"
        FROM ranked r
        JOIN nhp_rtdas_master m ON m.id = r."StationID"
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
