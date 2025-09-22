from pprint import pprint
from dotenv import dotenv_values
from pydruid.db import connect
from pydruid.db.exceptions import ProgrammingError
from requests import ConnectionError
import pandas as pd
from datetime import datetime, timedelta
from django.utils.timezone import now
from .utils import (
    gfs_rf, rf_anacim, rf_iri_seasonal, rf_anacim_subseasonal, rf_anacim_seasonal, rf_iri,
    gfs_temp, gfs_rh
)


todays_date = now().strftime("%Y-%m-%d")
env = dict(dotenv_values())
druid = {
    "host": env["DRUID_HOST"],
    "port": env["DRUID_PORT"],
    "path": env["DRUID_PATH"],
    "scheme": env["DRUID_SCHEME"],
}
data_source = {
    "5": "senegal-gfs-data",
    "6": "anacim-3-day-forecast-grid-data",
    "7": "senegal-iri-subx-data",
    "8": "senegal-iri-subx-data",
    "9": "senegal-iri-subx-data",
    "10": "senegal-iri-nmme-data",
    "11": "senegal-sub-x-data",  # ANACIM sub seasonal
    "12": "senegal-seasonal-data", # ANACIM seasonal
}



def get_rainfall_forecast(**kwargs):
    admin_level = kwargs.get("admin_level")
    admin_level_id = kwargs.get("admin_level_id") 
    data_src_table = data_source[kwargs.get('data_src_table')]
    try:
        if kwargs.get('data_src_table') == "5":
            # gfs
            # fields = (
            #     "total_precipitation" if admin_level == "commune"
            #     else f"""AVG("total_precipitation")"""
            # )
            fields = f"""AVG("total_precipitation")"""
            query = f"""
                SELECT "valid_time", {fields}
                FROM "druid"."senegal-gfs-data" WHERE TRUE
                AND "{admin_level}_id"='{admin_level_id}'
                AND "valid_time" NOT LIKE '%00:00%'
                GROUP BY "valid_time", "{admin_level}_id"
                ORDER BY "valid_time"
            """
            with connect(host=druid.get("host"), port=druid.get("port"), path=druid.get("path"), scheme=druid.get("scheme")) as connection:
                query_result = pd.DataFrame(connection.execute(query), dtype=object).to_records()
                result = gfs_rf(referred_date=todays_date, query_result=query_result)
                return {"status": 1, "data": result}
        elif kwargs.get('data_src_table') == "6":
            # anacim 3-day
            fields = f"""AVG("grid_rainfall")"""
            query = f"""
                WITH latest_times AS (
                    SELECT "__time"
                    FROM "druid"."anacim-3-day-forecast-grid-data"
                    WHERE "{admin_level}_id"='{admin_level_id}'
                    GROUP BY "__time"
                    ORDER BY "__time" DESC
                    LIMIT 3
                )
                SELECT d."__time", {fields} AS avg_rainfall
                FROM "druid"."anacim-3-day-forecast-grid-data" d
                JOIN latest_times t ON d."__time" = t."__time"
                WHERE d."{admin_level}_id"='{admin_level_id}'
                GROUP BY d."__time"
                ORDER BY d."__time" DESC
            """
            with connect(
                host=druid.get("host"),
                port=druid.get("port"),
                path=druid.get("path"),
                scheme=druid.get("scheme"),
            ) as connection:
                query_result = pd.DataFrame(connection.execute(query), dtype=object).to_records()
                result = rf_anacim(query_result=query_result)
                return {"status": 1, "data": result}

        elif kwargs.get('data_src_table') == "10":
            # iri seasonal
            fields = (
                """ "prob" """ if admin_level == "commune"
                else f"""AVG("prob")"""
            )
            group_by_fields = (
                "" if admin_level == "commune"
                else f""" GROUP BY "__time", "forecast_period", "tertiary_class" """
            )
            query = f"""
                SELECT "__time", "forecast_period", {fields}, "tertiary_class" FROM "senegal-iri-nmme-data" 
                WHERE TRUE
                AND "{admin_level}_id"='{admin_level_id}'
                AND "__time"=(SELECT MAX("__time") FROM "senegal-iri-nmme-data" WHERE TRUE AND "{admin_level}_id"='{admin_level_id}')
                {group_by_fields}
            """
            with connect(host=druid.get("host"), port=druid.get("port"), path=druid.get("path"), scheme=druid.get("scheme")) as connection:
                query_result = pd.DataFrame(connection.execute(query), dtype=object).to_records()
                result = rf_iri_seasonal(query_result=query_result)
                return {"status": 1, "data": result}
        elif kwargs.get('data_src_table') == "11":
            # anacim subseasonal
            fields = (
                """ "probabilistic", "deterministic" """ if admin_level == "commune"
                else f""" AVG("probabilistic"), AVG("deterministic") """
            )
            group_by_fields = (
                f""" GROUP BY "__time", "tertiary_class", "week", "probabilistic", "deterministic" """
                if admin_level == "commune"
                else f""" GROUP BY "__time", "tertiary_class", "week" """
            )
            query = f"""
                SELECT "__time", "tertiary_class", "week", {fields}
                FROM "senegal-sub-x-data"
                WHERE TRUE
                AND "{admin_level}_id"='{admin_level_id}'
                AND "__time"=(SELECT MAX("__time") FROM "senegal-sub-x-data" WHERE "{admin_level}_id"='{admin_level_id}')
                {group_by_fields}
                ORDER BY "week"
            """
            with connect(host=druid.get("host"), port=druid.get("port"), path=druid.get("path"), scheme=druid.get("scheme")) as connection:
                query_result = pd.DataFrame(connection.execute(query), dtype=object).to_records()
                result = rf_anacim_subseasonal(query_result=query_result)
                return {"status": 1, "data": result}
        elif kwargs.get('data_src_table') == "12":
            # anacim seasonal
            fields = (
                """ "probabilistic", "deterministic" """ if admin_level == "commune"
                else f""" AVG("probabilistic"), AVG("deterministic") """
            )
            group_by_fields = (
                f""" GROUP BY "__time", "tertiary_class", "period", "probabilistic", "deterministic" """
                if admin_level == "commune"
                else f""" GROUP BY "__time", "tertiary_class", "period" """
            )
            query = f"""
                SELECT "__time", "tertiary_class", "period", {fields}
                FROM "senegal-seasonal-data"
                WHERE TRUE
                AND "{admin_level}_id"='{admin_level_id}'
                AND "__time"=(SELECT MAX("__time") FROM "senegal-seasonal-data" WHERE "{admin_level}_id"='{admin_level_id}')
                {group_by_fields}
                ORDER BY "period"
            """
            with connect(host=druid.get("host"), port=druid.get("port"), path=druid.get("path"), scheme=druid.get("scheme")) as connection:
                query_result = pd.DataFrame(connection.execute(query), dtype=object).to_records()
                result = rf_anacim_seasonal(query_result=query_result)
                return {"status": 1, "data": result}
        else:
            # iri subseasonal
            column = {
                "7": "CFSv2_precipitation",
                "8": "ESRL_precipitation",
                "9": "GEFSv12_precipitation"
            }
            # fields = (
            #     f""" "{column[kwargs.get('data_src_table')]}" """ if admin_level == "commune"
            #     else f""" AVG("{column[kwargs.get('data_src_table')]}") """
            # )
            fields = f""" AVG("{column[kwargs.get('data_src_table')]}") """
            query = f"""
                SELECT "forecast_period", {fields}
                FROM "druid"."senegal-iri-subx-data"
                WHERE TRUE 
                AND "{admin_level}_id"='{admin_level_id}'
                AND "__time"=(SELECT MAX("__time") FROM "druid"."senegal-iri-subx-data" WHERE "{admin_level}_id"='{admin_level_id}')
                GROUP BY "{admin_level}_id", "forecast_period"
            """
            with connect(host=druid.get("host"), port=druid.get("port"), path=druid.get("path"), scheme=druid.get("scheme")) as connection:
                query_result = pd.DataFrame(connection.execute(query), dtype=object).to_records()
                result = rf_iri(query_result=query_result)
                return {"status": 1, "data": result}
    except ProgrammingError:
        return {"status": 0, "message": "Issue with aggregation, please check the query"}
    except ConnectionError:
        return {"status": 0, "message": f"Couldn't connect to database ({data_src_table}), please try again"}
    except (IndexError, KeyError):
        return {"status": 0, "message": "Data is unavailable at the source"}


def get_temperature_forecast(**kwargs):
    admin_level = kwargs.get("admin_level")
    admin_level_id = kwargs.get("admin_level_id") 
    data_src_table = data_source[kwargs.get('data_src_table')]
    try:
        if kwargs.get('data_src_table') == "5":
            # gfs
            # fields = (
            #     f""" "tmax", "tmin" """ if admin_level == "commune"
            #     else f""" AVG("tmax"), AVG("tmin") """
            # )
            fields = f""" AVG("tmax"), AVG("tmin") """
            query = f"""
                SELECT "valid_time", {fields}
                FROM "senegal-gfs-data" WHERE TRUE  AND "{admin_level}_id"='{admin_level_id}'
                AND "__time"=(SELECT MAX("__time") FROM "senegal-gfs-data" WHERE "{admin_level}_id"='{admin_level_id}')
                AND "valid_time" NOT LIKE '%00:00%'
                GROUP BY "valid_time", "{admin_level}_id"
                ORDER BY "valid_time"
            """
            print(query)
            with connect(host=druid.get("host"), port=druid.get("port"), path=druid.get("path"), scheme=druid.get("scheme")) as connection:
                query_result = pd.DataFrame(connection.execute(query), dtype=object).to_records()
                result = gfs_temp(referred_date=todays_date, query_result=query_result)
                return {"status": 1, "data": result}
    except ProgrammingError:
        return {"status": 0, "message": "Issue with SQL query, please check"}
    except ConnectionError:
        return {"status": 0, "message": f"Couldn't connect to database ({data_src_table}), please try again"}
    except (IndexError, KeyError):
        return {"status": 0, "message": "Data is unavailable at the source"}


def get_humidity_forecast(**kwargs):
    admin_level = kwargs.get("admin_level")
    admin_level_id = kwargs.get("admin_level_id") 
    data_src_table = data_source[kwargs.get('data_src_table')]
    try:
        if kwargs.get('data_src_table') == "5":
            # gfs
            # fields = (
            #     "relative_humidity_at_2m" if admin_level == "commune"
            #     else f""" AVG("relative_humidity_at_2m") """
            # )
            fields = f""" AVG("relative_humidity_at_2m") """
            query = f"""
                SELECT "valid_time", {fields}
                FROM "senegal-gfs-data" WHERE TRUE  AND "{admin_level}_id"='{admin_level_id}' 
                AND "__time"=(SELECT MAX("__time") FROM "senegal-gfs-data" WHERE "{admin_level}_id"='{admin_level_id}')
                AND "valid_time" NOT LIKE '%00:00%'
                GROUP BY "valid_time", "{admin_level}_id"
                ORDER BY "valid_time"
            """
            with connect(host=druid.get("host"), port=druid.get("port"), path=druid.get("path"), scheme=druid.get("scheme")) as connection:
                query_result = pd.DataFrame(connection.execute(query), dtype=object).to_records()
                result = gfs_rh(referred_date=todays_date, query_result=query_result)
                return {"status": 1, "data": result}
    except ProgrammingError:
        return {"status": 0, "message": "Issue with SQL query, please check"}
    except ConnectionError:
        return {"status": 0, "message": f"Couldn't connect to database ({data_src_table}), please try again"}
    except (IndexError, KeyError):
        return {"status": 0, "message": "Data is unavailable at the source"}
