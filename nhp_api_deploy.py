# from fastapi import FastAPI, Depends, HTTPException, Query
# from fastapi.security import HTTPBasic, HTTPBasicCredentials
# from typing import List, Optional
# import psycopg2
# import os
# from dotenv import load_dotenv
# import pandas as pd
# from sqlalchemy import create_engine, text

# # Load env
# load_dotenv()

# DB_CONFIG = {
#     "host": os.getenv("DB_HOST"),
#     "port": os.getenv("DB_PORT"),
#     "dbname": os.getenv("DB_NAME"),
#     "user": os.getenv("DB_USER"),
#     "password": os.getenv("DB_PASSWORD"),
# }

# # Basic Auth
# security = HTTPBasic()
# USERNAME = os.getenv("API_USER")
# PASSWORD = os.getenv("API_PASS")

# app = FastAPI(title="NHP RTDAS API", version="1.0")

# def get_current_user(credentials: HTTPBasicCredentials = Depends(security)):
#     if credentials.username != USERNAME or credentials.password != PASSWORD:
#         raise HTTPException(status_code=401, detail="Unauthorized")
#     return credentials.username


# #--------------------BASIC-----------------------------------------------------------------------------------------------

# # def get_connection():
# #     return psycopg2.connect(**DB_CONFIG)

# # @app.get("/stations/data")
# # def get_station_data(
# #     date: Optional[str] = None,
# #     station_type: Optional[str] = None,
# #     basin: Optional[str] = None,
# #     user: str = Depends(get_current_user)
# # ):
# #     """
# #     Returns station + RTDAS data
# #     Filters: date (YYYY-MM-DD), station_type (AWLR, AWLR_ARG), basin (Beki, Jiadhal, Buridehing)
# #     """

# #     query = f"""
# #         SELECT
# #             m.id AS station_id,
# #             m.longitude,
# #             m.latitude,
# #             m.basin,
# #             m.name,
# #             m.type,
# #             d."MobileNumber",
# #             d."Battery",
# #             d."WaterLevel",
# #             d."HourlyRain",
# #             d."DailyRainfall"
# #         FROM nhp_rtdas_master m
# #         JOIN nhp_rtdas_ingest d ON m.id = d."StationID"
# #         WHERE 1=1
# #     """

# #     params = []
# #     if date:
# #         query += ' AND d."DateTime"::date = %s'
# #         params.append(date)
# #     if station_type:
# #         query += " AND m.type = %s"
# #         params.append(station_type)
# #     if basin:
# #         query += " AND m.basin = %s"
# #         params.append(basin)

# #     with get_connection() as conn:
# #         df = pd.read_sql(query, conn, params=params)

# #     return df.to_dict(orient="records")

# #---------------------WITH PAGINATION--------------------------------------------------------------------------------------------------------------------------------------------

# # def get_connection():
# #     return psycopg2.connect(**DB_CONFIG)

# # @app.get("/stations/data")
# # def get_station_data(
# #     date: Optional[str] = Query(None, description="Filter by date (YYYY-MM-DD)"),
# #     station_type: Optional[str] = Query(None, description="Filter by station type"),
# #     basin: Optional[str] = Query(None, description="Filter by basin name"),
# #     page: Optional[int] = Query(None, ge=1, description="Page number (default 1)"),
# #     page_size: Optional[int] = Query(None, ge=1, le=500, description="Records per page (default 50)"),
# #     user: str = Depends(get_current_user)
# # ):
# #     """
# #     Fetch paginated RTDAS + master station data.
# #     Filters: date, station_type, basin
# #     Pagination: optional (?page=2&page_size=100)
# #     """

# #     # === Default pagination if not provided ===
# #     page = page or 1
# #     page_size = page_size or 50

# #     query = f"""
# #         SELECT
# #             m.id AS station_id,
# #             m.longitude,
# #             m.latitude,
# #             m.basin,
# #             m.name,
# #             m.type,
# #             d."MobileNumber",
# #             d."Battery",
# #             d."WaterLevel",
# #             d."HourlyRain",
# #             d."DailyRainfall",
# #             d."DateTime"
# #         FROM nhp_rtdas_master m
# #         JOIN nhp_rtdas_ingest d ON m.id = d."StationID"
# #         WHERE 1=1
# #     """

# #     params = []
# #     if date:
# #         query += ' AND d."DateTime"::date = %s'
# #         params.append(date)
# #     if station_type:
# #         query += " AND m.type = %s"
# #         params.append(station_type)
# #     if basin:
# #         query += " AND m.basin = %s"
# #         params.append(basin)

# #     # Order + pagination
# #     query += ' ORDER BY d."DateTime" DESC LIMIT %s OFFSET %s'
# #     params.extend([page_size, (page - 1) * page_size])

# #     with get_connection() as conn:
# #         df = pd.read_sql(query, conn, params=params)

# #         # Count query for pagination metadata
# #         count_query = f"""
# #             SELECT COUNT(*) FROM nhp_rtdas_master m
# #             JOIN nhp_rtdas_ingest d ON m.id = d."StationID"
# #             WHERE 1=1
# #         """
# #         count_params = []
# #         if date:
# #             count_query += ' AND d."DateTime"::date = %s'
# #             count_params.append(date)
# #         if station_type:
# #             count_query += " AND m.type = %s"
# #             count_params.append(station_type)
# #         if basin:
# #             count_query += " AND m.basin = %s"
# #             count_params.append(basin)

# #         total_records = pd.read_sql(count_query, conn, params=count_params).iloc[0, 0]

# #     return {
# #         "page": page,
# #         "page_size": page_size,
# #         "total_records": int(total_records),
# #         "total_pages": (int(total_records) + page_size - 1) // page_size,
# #         "data": df.to_dict(orient="records")
# #     }

# #-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# # # === SQLAlchemy engine ===
# # engine_url = (
# #     f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
# #     f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
# # )
# # engine = create_engine(engine_url)

# # @app.get("/stations/data")
# # def get_station_data(
# #     date: Optional[str] = Query(None, description="Filter by date (YYYY-MM-DD)"),
# #     station_type: Optional[str] = Query(None, description="Filter by station type"),
# #     basin: Optional[str] = Query(None, description="Filter by basin name"),
# #     page: Optional[int] = Query(None, ge=1, description="Page number (default 1)"),
# #     page_size: Optional[int] = Query(None, ge=1, le=500, description="Records per page (default 50)"),
# #     user: str = Depends(get_current_user)
# # ):
# #     """Fetch paginated RTDAS + master station data."""

# #     # Default pagination
# #     page = page or 1
# #     page_size = page_size or 50

# #     # === Main query ===
# #     base_query = """
# #         FROM nhp_rtdas_master m
# #         JOIN nhp_rtdas_ingest d ON m.id = d."StationID"
# #         WHERE 1=1
# #     """

# #     filters = []
# #     params = {}

# #     if date:
# #         filters.append('AND d."DateTime"::date = :date')
# #         params["date"] = date
# #     if station_type:
# #         filters.append("AND m.type = :station_type")
# #         params["station_type"] = station_type
# #     if basin:
# #         filters.append("AND m.basin = :basin")
# #         params["basin"] = basin

# #     # === Data query with pagination ===
# #     data_query = text(f"""
# #         SELECT
# #             m.id AS station_id,
# #             m.longitude,
# #             m.latitude,
# #             m.basin,
# #             m.name,
# #             m.type,
# #             d."MobileNumber",
# #             d."Battery",
# #             d."WaterLevel",
# #             d."HourlyRain",
# #             d."DailyRainfall",
# #             d."DateTime"
# #         {base_query}
# #         {' '.join(filters)}
# #         ORDER BY d."DateTime" DESC
# #         LIMIT :limit OFFSET :offset
# #     """)

# #     params["limit"] = page_size
# #     params["offset"] = (page - 1) * page_size

# #     # === Count query ===
# #     count_query = text(f"""
# #         SELECT COUNT(*) AS total
# #         {base_query}
# #         {' '.join(filters)}
# #     """)

# #     with engine.connect() as conn:
# #         df = pd.read_sql(data_query, conn, params=params)
# #         total_records = conn.execute(count_query, params).scalar() or 0

# #     return {
# #         "page": page,
# #         "page_size": page_size,
# #         "total_records": int(total_records),  # Fix numpy.int64 issue
# #         "total_pages": (int(total_records) + page_size - 1) // page_size,
# #         "data": df.to_dict(orient="records"),
# #     }

# #-------------------------------------------------------------------------------------------------------------------------------------------------------------------
# # === SQLAlchemy engine ===
# engine_url = (
#     f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
#     f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
# )
# engine = create_engine(engine_url)

# #============Main data endpoint=================

# @app.get("/stations/data")
# def get_station_data(
#     start_date: Optional[str] = Query(None, description="Filter by start date (YYYY-MM-DD, DD-MM-YYYY)"),
#     end_date: Optional[str] = Query(None, description="Filter by end date (YYYY-MM-DD, DD-MM-YYYY)"),
#     station_type: Optional[str] = Query(None, description="Filter by station type (AWLR, AWLR_ARG)"),
#     basin: Optional[str] = Query(None, description="Filter by basin name (Beki, Jiadhal, Buridehing)"),
#     page: Optional[int] = Query(None, ge=1, description="Page number (default 1)"),
#     page_size: Optional[int] = Query(None, ge=1, le=500, description="Records per page (default 50)"),
#     user: str = Depends(get_current_user),
# ):
#     """
#     Fetch RTDAS + master station data with optional filters:
#     - Date range (start_date, end_date)
#     - Station_type/Basin
#     - Pagination
#     - Latest record per station
#     """

#     # === Default pagination ===
#     page = page or 1
#     page_size = page_size or 50

#     # === Base query ===
#     base_query = """
#         FROM nhp_rtdas_master m
#         JOIN nhp_rtdas_ingest d ON m.id = d."StationID"
#         WHERE 1=1
#     """

#     filters = []
#     params = {}

#     # === Date range filters ===
#     if start_date and end_date:
#         filters.append('AND d."DateTime"::date BETWEEN :start_date AND :end_date')
#         params["start_date"] = start_date
#         params["end_date"] = end_date
#     elif start_date:
#         filters.append('AND d."DateTime"::date >= :start_date')
#         params["start_date"] = start_date
#     elif end_date:
#         filters.append('AND d."DateTime"::date <= :end_date')
#         params["end_date"] = end_date

#     # === Additional filters ===
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
#         "total_records": int(total_records),
#         "total_pages": (int(total_records) + page_size - 1) // page_size,
#         "data": df.to_dict(orient="records"),
#     }

# #===============Latest record endpoint==========================


# @app.get("/stations/latest")
# def get_latest_station_data(
#     station_type: Optional[str] = Query(None, description="Filter by station type"),
#     basin: Optional[str] = Query(None, description="Filter by basin name"),
#     limit: Optional[int] = Query(1, ge=1, le=20, description="Number of latest records per station (default 1, max 20)"),
#     user: str = Depends(get_current_user),
# ):
#     """
#     Fetch latest N records per station (default = 1, max = 20).
#     Optional filters: station_type, basin.
#     """

#     params = {"limit": limit}
#     meta_filters = []

#     # Filters on master table
#     if station_type:
#         meta_filters.append("AND m.type = :station_type")
#         params["station_type"] = station_type
#     if basin:
#         meta_filters.append("AND m.basin = :basin")
#         params["basin"] = basin

#     meta_where = " ".join(meta_filters)

#     query = text(f"""
#         WITH ranked AS (
#             SELECT
#                 d."StationID",
#                 d."DateTime",
#                 d."MobileNumber",
#                 d."Battery",
#                 d."WaterLevel",
#                 d."HourlyRain",
#                 d."DailyRainfall",
#                 ROW_NUMBER() OVER (PARTITION BY d."StationID" ORDER BY d."DateTime"::timestamp DESC) AS rn
#             FROM nhp_rtdas_ingest d
#         )
#         SELECT
#             m.id AS station_id,
#             m.longitude,
#             m.latitude,
#             m.basin,
#             m.name,
#             m.type,
#             r."MobileNumber",
#             r."Battery",
#             r."WaterLevel",
#             r."HourlyRain",
#             r."DailyRainfall",
#             r."DateTime"
#         FROM ranked r
#         JOIN nhp_rtdas_master m ON m.id = r."StationID"
#         WHERE r.rn <= :limit {meta_where}
#         ORDER BY r."StationID", r."DateTime" DESC;
#     """)

#     with engine.connect() as conn:
#         df = pd.read_sql(query, conn, params=params)

#     return {
#         "limit_per_station": limit,
#         "total_records": len(df),
#         "data": df.to_dict(orient="records"),
#     }

#=============================================================================================================================================================================


# from fastapi import FastAPI, Depends, HTTPException, Query
# from fastapi.security import HTTPBasic, HTTPBasicCredentials
# from typing import List, Optional
# import os
# from dotenv import load_dotenv
# import pandas as pd
# from sqlalchemy import create_engine, text

# # Load env
# load_dotenv()

# DB_CONFIG = {
#     "host": os.getenv("DB_HOST"),
#     "port": os.getenv("DB_PORT"),
#     "dbname": os.getenv("DB_NAME"),
#     "user": os.getenv("DB_USER"),
#     "password": os.getenv("DB_PASSWORD"),
# }

# # Basic Auth
# security = HTTPBasic()
# USERNAME = os.getenv("API_USER")
# PASSWORD = os.getenv("API_PASS")

# app = FastAPI(title="NHP RTDAS API", version="1.0")

# def get_current_user(credentials: HTTPBasicCredentials = Depends(security)):
#     if credentials.username != USERNAME or credentials.password != PASSWORD:
#         raise HTTPException(status_code=401, detail="Unauthorized")
#     return credentials.username

# # === SQLAlchemy engine ===
# engine_url = (
#     f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
#     f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
# )
# engine = create_engine(engine_url)


# # ------------------ Helper: case-insensitive LIKE pattern ------------------
# def make_pattern(value: str) -> str:
#     """
#     Normalize and create a fuzzy search pattern.
#     Converts 'kamrup' -> '%kamrup%'
#     Removes special chars for more flexible matching.
#     """
#     value = value.strip().lower()
#     #value = value.replace("-", "").replace("(", "").replace(")", "")
#     for ch in ['-', '(', ')', ',']:
#         value = value.replace(ch, '')
#     return f"%{value}%"


# #============Main data endpoint=================

# @app.get("/stations/data")
# def get_station_data(
#     start_date: Optional[str] = Query(None, description="Filter by start date (YYYY-MM-DD, DD-MM-YYYY)"),
#     end_date: Optional[str] = Query(None, description="Filter by end date (YYYY-MM-DD, DD-MM-YYYY)"),
#     station_type: Optional[str] = Query(None, description="Filter by station type (AWLR, ARG, AWS, ARG+AWLR)"),
#     zone: Optional[str] = Query(None, description="Filter by zone name"),
#     location: Optional[str] = Query(None, description="Filter by location name"),
#     district: Optional[str] = Query(None, description="Filter by district name"),
#     page: Optional[int] = Query(None, ge=1, description="Page number (default 1)"),
#     page_size: Optional[int] = Query(None, ge=1, le=500, description="Records per page (default 50)"),
#     user: str = Depends(get_current_user),
# ):
#     """
#     Fetch RTDAS + master station data with optional fuzzy normalized filters:
#     - Date range (start_date, end_date)
#     - Station_type/Zone/location/District
#     - Pagination
#     """

#     # === Default pagination ===
#     page = page or 1
#     page_size = page_size or 50

#     # === Base query ===
#     base_query = """
#         FROM nhp_v2 m
#         JOIN nhp_rtdas_ingest_v1 d ON m.id = d."StationID"
#         WHERE 1=1
#     """

#     filters = []
#     params = {}

#     # === Date range filters ===
#     if start_date and end_date:
#         filters.append('AND d."DateTime"::date BETWEEN :start_date AND :end_date')
#         params["start_date"] = start_date
#         params["end_date"] = end_date
#     elif start_date:
#         filters.append('AND d."DateTime"::date >= :start_date')
#         params["start_date"] = start_date
#     elif end_date:
#         filters.append('AND d."DateTime"::date <= :end_date')
#         params["end_date"] = end_date

#     # === Additional filters ===
  
#     if district:
#         filters.append("AND LOWER(m.district) ILIKE :district")
#         params["district"] = make_pattern(district)
#     if location:
#         filters.append("AND LOWER(m.location) ILIKE :location")
#         params["location"] = make_pattern(location)
#     if zone:
#         filters.append("AND LOWER(m.zone) ILIKE :zone")
#         params["zone"] = make_pattern(zone)
#     if station_type:
#         filters.append("AND LOWER(m.type) ILIKE :station_type")
#         params["station_type"] = make_pattern(station_type)
    

#     # === Data query with pagination ===
#     data_query = text(f"""
#         SELECT
#             m.id AS station_id,
#             m.longitude,
#             m.latitude,
#             m.zone,
#             m.name,
#             m.type,
#             m.location,
#             m.district,                    
#             d."MobileNumber",
#             d."Battery",
#             d."WaterLevel",
#             d."HourlyRain",
#             d."DailyRain",
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
#         "total_records": int(total_records),
#         "total_pages": (int(total_records) + page_size - 1) // page_size,
#         "data": df.to_dict(orient="records"),
#     }

# #===============Latest record endpoint==========================


# @app.get("/stations/latest")
# def get_latest_station_data(
#     station_type: Optional[str] = Query(None, description="Filter by station type"),
#     zone: Optional[str] = Query(None, description="Filter by zone name"),
#     district: Optional[str] = Query(None, description="Filter by district name"),
#     location: Optional[str] = Query(None, description="Filter by location name"),
#     limit: Optional[int] = Query(1, ge=1, le=20, description="Number of latest records per station (default 1, max 20)"),
#     user: str = Depends(get_current_user),
# ):
#     """
#     Fetch latest N records per station (default = 1, max = 20).
#     Optional fuzzy normalized filters: station_type, zone, district, location.
#     """

#     params = {"limit": limit}
#     meta_filters = []

#     # Filters on master table
    
#     if district:
#         meta_filters.append("AND LOWER(m.district) ILIKE :district")
#         params["district"] = make_pattern(district)
#     if location:
#         meta_filters.append("AND LOWER(m.location) ILIKE :location")
#         params["location"] = make_pattern(location)
#     if zone:
#         meta_filters.append("AND LOWER(m.zone) ILIKE :zone")
#         params["zone"] = make_pattern(zone)
#     if station_type:
#         meta_filters.append("AND LOWER(m.type) ILIKE :station_type")
#         params["station_type"] = make_pattern(station_type)

#     meta_where = " ".join(meta_filters)

#     query = text(f"""
#         WITH ranked AS (
#             SELECT
#                 d."StationID",
#                 d."DateTime",
#                 d."MobileNumber",
#                 d."Battery",
#                 d."WaterLevel",
#                 d."HourlyRain",
#                 d."DailyRain",
#                 ROW_NUMBER() OVER (PARTITION BY d."StationID" ORDER BY d."DateTime"::timestamp DESC) AS rn
#             FROM nhp_rtdas_ingest_v1 d
#         )
#         SELECT
#             m.id AS station_id,
#             m.longitude,
#             m.latitude,
#             m.zone,
#             m.name,
#             m.type,
#             m.location,
#             m.district,          
#             r."MobileNumber",
#             r."Battery",
#             r."WaterLevel",
#             r."HourlyRain",
#             r."DailyRainfall",
#             r."DateTime"
#         FROM ranked r
#         JOIN nhp_v2 m ON m.id = r."StationID"
#         WHERE r.rn <= :limit {meta_where}
#         ORDER BY r."StationID", r."DateTime" DESC;
#     """)

#     with engine.connect() as conn:
#         df = pd.read_sql(query, conn, params=params)

#     return {
#         "limit_per_station": limit,
#         "total_records": len(df),
#         "data": df.to_dict(orient="records"),
#     }


# #=====================Station Meta data endpoint=================================================================================

# @app.get("/master/filter")
# def get_filtered(
#     district: Optional[str] = Query(None, description="District name (case-insensitive)"),
#     location: Optional[str] = Query(None, description="Location name (case-insensitive)"),
#     zone: Optional[str] = Query(None, description="Zone name (case-insensitive)"),
#     station_type: Optional[str] = Query(None, description="Sensor type (case-insensitive)"),
#     user: str = Depends(get_current_user)
# ):
#     """
#     Fetch meta data.
#     Optional fuzzy normalized filters: station_type, zone, district, location.
#     """

#     filters = []
#     params = {}

#     if district:
#         filters.append("AND LOWER(district) ILIKE :district")
#         params["district"] = make_pattern(district)
#     if location:
#         filters.append("AND LOWER(location) ILIKE :location")
#         params["location"] = make_pattern(location)
#     if zone:
#         filters.append("AND LOWER(zone) ILIKE :zone")
#         params["zone"] = make_pattern(zone)
#     if station_type:
#         filters.append("AND LOWER(type) ILIKE :station_type")
#         params["station_type"] = make_pattern(station_type)


#     query = text(f"""
#         SELECT id, district, name, location, zone, latitude, longitude, type
#         FROM nhp_v2
#         WHERE 1=1 {' '.join(filters)}
#         ORDER BY "id";
#     """)
    
#     try:
#         with engine.connect() as conn:
#             df = pd.read_sql(query, conn, params=params)
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
#     return {
#         "total_records": len(df),
#         "meta data": df.to_dict(orient="records"),
#     }
    
#===================================================================================================================================================================
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

app = FastAPI(title="NHP RTDAS API", version="1.2")


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
    Removes common punctuation for flexible matching and returns %value%.
    """
    value = value.strip().lower()
    for ch in ['-', '(', ')', ',']:
        value = value.replace(ch, '')
    return f"%{value}%"


# ------------------ Helpers: field lists ------------------
# Base fields (6) that every station type should show:
BASE_FIELDS = [
    "MobileNumber",
    "Battery",
    "WaterLevel",
    "HourlyRain",
    "DailyRain",
    "DateTime",
]

# AWS extra fields (8)
AWS_EXTRA_FIELDS = [
    "AT",
    "SnowDepth",
    "Evaporation",
    "WS",
    "WD",
    "At.pressure",     # quoted identifier
    "RH",
    "Sun Radiation"    # quoted identifier with space
]

# Build SELECT fragments (with proper quoting)
def dq(col: str) -> str:
    """Double-quote identifier for SQL if it contains spaces or dots; else safe-quote it."""
    # if already contains quote, return as-is
    if '"' in col:
        return col
    return f'"{col}"'


# ============= Main data endpoint =================
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
    Fetch RTDAS + master station data with optional fuzzy normalized filters:
    - Date range (start_date, end_date)
    - Station_type/Zone/location/District
    - Pagination

    AWS stations will include the 14-field payload (base 6 + 8 AWS extras).
    Other station types will include the 6-field payload.
    """

    # Defaults
    page = page or 1
    page_size = page_size or 50

    base_query = """
        FROM nhp_v2 m
        JOIN nhp_rtdas_ingest_v1 d ON m.id = d."StationID"
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

    # === Additional meta filters (fuzzy ILIKE) ===
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

    # === Build SELECT: master columns + all possible ingest columns (base + aws extras) ===
    master_cols = [
        "m.id AS station_id",
        "m.longitude",
        "m.latitude",
        "m.zone",
        "m.name",
        "m.type",
        "m.location",
        "m.district"
    ]

    ingest_cols = [f'd.{dq(f)}' for f in BASE_FIELDS + AWS_EXTRA_FIELDS]  # d."col" for each

    select_clause = ",\n            ".join(master_cols + ingest_cols)

    # === Data query with pagination ===
    data_query = text(f"""
        SELECT
            {select_clause}
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

    # === Post-process per-row to respect AWS vs others ===
    records = []
    for r in df.to_dict(orient="records"):
        stype = (r.get("type") or "").strip().lower()
        out = {
            "station_id": r.get("station_id"),
            "longitude": r.get("longitude"),
            "latitude": r.get("latitude"),
            "zone": r.get("zone"),
            "name": r.get("name"),
            "type": r.get("type"),
            "location": r.get("location"),
            "district": r.get("district"),
        }

        # include base fields always (if present)
        for f in BASE_FIELDS:
            out[f] = r.get(f)

        # For AWS stations include the extra fields
        if stype == "aws":
            for f in AWS_EXTRA_FIELDS:
                # normalize key for JSON (replace spaces/dots with underscores)
                key = f.replace(" ", "_").replace(".", "_")
                out[key] = r.get(f)
        # For non-AWS, we don't include extra fields (they will be absent)
        records.append(out)

    return {
        "page": page,
        "page_size": page_size,
        "total_records": int(total_records),
        "total_pages": (int(total_records) + page_size - 1) // page_size,
        "data": records,
    }


# ================ Latest record endpoint ==========================
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
    AWS stations return 14 fields, others return base 6 fields.
    """

    params = {"limit": limit}
    meta_filters = []

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

    # Build ranked CTE selecting all possible fields
    all_ingest_cols = [f'd.{dq(f)}' for f in BASE_FIELDS + AWS_EXTRA_FIELDS]
    ingest_cols_clause = ", ".join(all_ingest_cols)

    query = text(f"""
        WITH ranked AS (
            SELECT
                d."StationID",
                {ingest_cols_clause},
                ROW_NUMBER() OVER (PARTITION BY d."StationID" ORDER BY d."DateTime"::timestamp DESC) AS rn
            FROM nhp_rtdas_ingest_v1 d
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
            r."StationID" as ingest_stationid,
            {ingest_cols_clause.replace('d.', 'r.')}
        FROM ranked r
        JOIN nhp_v2 m ON m.id = r."StationID"
        WHERE r.rn <= :limit {meta_where}
        ORDER BY r."StationID", r."DateTime" DESC;
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)

    # Post-process rows similar to /stations/data
    records = []
    for r in df.to_dict(orient="records"):
        stype = (r.get("type") or "").strip().lower()
        out = {
            "station_id": r.get("station_id"),
            "longitude": r.get("longitude"),
            "latitude": r.get("latitude"),
            "zone": r.get("zone"),
            "name": r.get("name"),
            "type": r.get("type"),
            "location": r.get("location"),
            "district": r.get("district"),
        }

        # base fields
        for f in BASE_FIELDS:
            out[f] = r.get(f)

        if stype == "aws":
            for f in AWS_EXTRA_FIELDS:
                key = f.replace(" ", "_").replace(".", "_")
                out[key] = r.get(f)

        records.append(out)

    return {
        "limit_per_station": limit,
        "total_records": len(records),
        "data": records,
    }


# ================= Station Meta data endpoint ==========================
@app.get("/master/filter")
def get_filtered(
    district: Optional[str] = Query(None, description="District name (case-insensitive)"),
    location: Optional[str] = Query(None, description="Location name (case-insensitive)"),
    zone: Optional[str] = Query(None, description="Zone name (case-insensitive)"),
    station_type: Optional[str] = Query(None, description="Sensor type (case-insensitive)"),
    user: str = Depends(get_current_user)
):
    """
    Fetch master/meta data with fuzzy normalized filters.
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
