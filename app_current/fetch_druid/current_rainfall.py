from pprint import pprint
from dotenv import dotenv_values
from pydruid.db import connect
from pydruid.db.exceptions import ProgrammingError
from requests import ConnectionError
import pandas as pd
from datetime import datetime, timedelta
from django.utils.timezone import now
from app_lookups.constants import met_week_long, month_list, dekad_list, week_list
from ..stats import get_curr_common_stats, is_non_leap_year


data_source = {
    "1": "senegal-enacts-data",
    "2": "senegal-tamsat-data",
    "3": "senegal-chirps-data",
    "4": "senegal-arc2-data",
}

env = dict(dotenv_values())
druid = {
    "host": env["DRUID_HOST"],
    "port": env["DRUID_PORT"],
    "path": env["DRUID_PATH"],
    "scheme": env["DRUID_SCHEME"],
}
current_year = now().year
current_year_filter = f""" AND "year"={current_year}"""
previous_year_filter = f""" AND "year"={current_year-1}"""
first_year = now().year - int(env["YEARS_AGO_FOR_FIRST_YEAR"])
year_filter = f""" AND "year">={first_year} AND "year"<={current_year-1}"""
current_day = now().day
current_month = now().month
current_week = next(i for i in met_week_long if i["day"] == current_day and i["month"] == current_month)["met_week"]

previous_day = (now() - timedelta(days=1)).day
previous_day_month = (now() - timedelta(days=1)).month



def get_current_monthly_rainfall(**kwargs):
    admin_level = kwargs.get("admin_level")
    admin_level_id = kwargs.get("admin_level_id") 
    from_month = kwargs.get("from_month")
    to_month = kwargs.get("to_month")
    data_src_table = data_source[kwargs.get('data_src_table')]
    month_filter = f""" AND "month_num">={from_month} AND "month_num"<={to_month} """ if from_month and to_month else ""
    try:
        # 1. actual available dates in the current year from the selected months
        date_query = f"""
            SELECT MIN("__time"), MAX("__time") FROM "druid"."{data_src_table}"
            WHERE TRUE AND "{admin_level}_id"='{admin_level_id}' {current_year_filter}{month_filter}
        """
        with connect(host=druid["host"], port=druid["port"], path=druid["path"], scheme=druid["scheme"]) as connection:
            date_query_result = pd.DataFrame(connection.execute(date_query), dtype=object).to_records()
            real_start_date = date_query_result[0][1][0:10]
            real_end_date = date_query_result[0][2][0:10]
            days_count = (datetime.strptime(real_end_date, "%Y-%m-%d") - datetime.strptime(real_start_date, "%Y-%m-%d")).days + 1
            start_month_day = real_start_date[5:]
            end_month_day = real_end_date[5:]
        # 2. current year monthly values
        query = f"""
            SELECT "commune_id", "month_num", ROUND(SUM("grid_rainfall"), 1) AS "sum", ROUND(AVG("grid_rainfall"), 1) AS "avg"
            FROM "druid"."{data_src_table}"
            WHERE TRUE AND "{admin_level}_id"='{admin_level_id}' {current_year_filter}{month_filter}
            GROUP BY "commune_id", "month_num"
        """
        with connect(host=druid["host"], port=druid["port"], path=druid["path"], scheme=druid["scheme"]) as connection:
            query_result = pd.DataFrame(connection.execute(query), dtype=object)
            if admin_level == "commune":
                monthly_rf_vals = [{
                    "month": i[2],
                    "month_text": list(filter(lambda x: x["id"] == i[2], month_list))[0]["month_text"],
                    "rainfall": i[3],
                    "mean_rf": i[4]
                } for i in query_result.to_records()]
            else:
                grouped = (
                    query_result.groupby("month_num")
                        .agg(rainfall=("sum", "mean"), mean_rf=("avg", "mean"))
                        .reset_index()
                )
                monthly_rf_vals = [
                    {
                        "month": row["month_num"],
                        "month_text": next(m["month_text"] for m in month_list if m["id"] == row["month_num"]),
                        "rainfall": round(row["rainfall"], 1),
                        "mean_rf": round(row["mean_rf"], 1),
                    }
                    for _, row in grouped.iterrows()
                ]
            for n, _ in enumerate(monthly_rf_vals):
                monthly_rf_vals[n]["cumulative_rf"] = round((monthly_rf_vals[n]["rainfall"] if n == 0 else monthly_rf_vals[n]["rainfall"] + monthly_rf_vals[n-1]["cumulative_rf"]), 1)
            monthly_rf_stats = get_curr_common_stats(list(map(lambda x: x["rainfall"], monthly_rf_vals)))
        # 3. rolling past 30 year -- using same dates
        with connect(host=druid["host"], port=druid["port"], path=druid["path"], scheme=druid["scheme"]) as connection:
            hist_rf_vals = []
            for y in range(current_year - 30, current_year):
                smd = "02-28" if is_non_leap_year(y) and start_month_day == "02-29" else start_month_day
                emd = "02-28" if is_non_leap_year(y) and end_month_day == "02-29" else end_month_day
                start_date = f"{y}-{smd}"
                end_date = f"{y}-{emd}"
                query = f"""
                    SELECT "commune_id", "year", ROUND(SUM("grid_rainfall"), 1) AS sum_rf, COUNT(*) AS day_count
                    FROM "druid"."{data_src_table}"
                    WHERE 1=1 AND "{admin_level}_id"='{admin_level_id}'
                    AND "year"={y} 
                    AND "__time">='{start_date}T00:00:00.000Z' 
                    AND "__time"<='{end_date}T00:00:00.000Z'
                    GROUP BY "commune_id", "year"
                """
                query_result = pd.DataFrame(connection.execute(query), dtype=object)
                if not query_result.empty:
                    if admin_level == "commune":
                        row = query_result.iloc[0]
                        hist_rf_vals.append({
                            "year": row["year"],
                            "rainfall": row["sum_rf"] or 0,
                            "day_count": row["day_count"]
                        })
                    else:
                        avg_rf = query_result["sum_rf"].mean()
                        avg_days = query_result["day_count"].mean()   # or first() if same for all
                        hist_rf_vals.append({
                            "year": y,
                            "rainfall": round(avg_rf, 1),
                            "day_count": int(avg_days)
                        })
            hist_rf_avg = (
                round(sum([x["rainfall"] for x in hist_rf_vals]) / len(hist_rf_vals), 1)
                if hist_rf_vals else "N/A"
            )
        # 4. send api response
        return {
            "status": 1, 
            "data": {
                "cy_data": {
                    "cy_start_date": real_start_date,
                    "cy_end_date": real_end_date,
                    "days_count": days_count,
                    "monthly_rf_vals": monthly_rf_vals,
                    "monthly_rf_stats": monthly_rf_stats
                },
                "py_data": {
                    "hist_rf_vals": hist_rf_vals,
                    "hist_rf_avg": hist_rf_avg
                }
            }
        }
    except ProgrammingError:
        return {"status": 0, "message": "Issue with SQL query, please check"}
    except ConnectionError:
        return {"status": 0, "message": f"Couldn't connect to database ({data_src_table}), please try again"}



def get_current_dekadal_rainfall(**kwargs):
    admin_level = kwargs.get("admin_level")
    admin_level_id = kwargs.get("admin_level_id") 
    from_dekad = kwargs.get("from_dekad")
    to_dekad = kwargs.get("to_dekad")
    data_src_table = data_source[kwargs.get('data_src_table')]
    dekad_filter = f""" AND "dekad_week_num">={from_dekad} AND "dekad_week_num"<={to_dekad}""" if from_dekad and to_dekad else ""
    try:
        # 1. actual available dates in the current year from the selected months
        date_query = f"""
            SELECT MIN("__time"), MAX("__time") FROM "druid"."{data_src_table}"
            WHERE TRUE AND "{admin_level}_id"='{admin_level_id}' {current_year_filter}{dekad_filter}
        """
        with connect(host=druid["host"], port=druid["port"], path=druid["path"], scheme=druid["scheme"]) as connection:
            date_query_result = pd.DataFrame(connection.execute(date_query), dtype=object).to_records()
            real_start_date = date_query_result[0][1][0:10]
            real_end_date = date_query_result[0][2][0:10]
            days_count = (datetime.strptime(real_end_date, "%Y-%m-%d") - datetime.strptime(real_start_date, "%Y-%m-%d")).days + 1
            start_month_day = real_start_date[5:]
            end_month_day = real_end_date[5:]
        # 2. current year monthly values
        query = f"""
            SELECT "commune_id", "dekad_week_num", ROUND(SUM("grid_rainfall"), 1) AS "sum", ROUND(AVG("grid_rainfall"), 1) AS "avg"
            FROM "druid"."{data_src_table}"
            WHERE TRUE AND "{admin_level}_id"='{admin_level_id}' {current_year_filter}{dekad_filter}
            GROUP BY "commune_id", "dekad_week_num"
        """
        with connect(host=druid["host"], port=druid["port"], path=druid["path"], scheme=druid["scheme"]) as connection:
            query_result = pd.DataFrame(connection.execute(query), dtype=object)
            if admin_level == "commune":
                dekad_rf_vals = [{
                    "dekad": i[2],
                    "dekad_text": list(filter(lambda x: x["id"] == i[2], dekad_list))[0]["dekad_text"],
                    "rainfall": i[3],
                    "mean_rf": i[4]
                } for i in query_result.to_records()]
            else:
                grouped = (
                    query_result.groupby("dekad_week_num")
                        .agg(rainfall=("sum", "mean"), mean_rf=("avg", "mean"))
                        .reset_index()
                )
                dekad_rf_vals = [
                    {
                        "dekad": row["dekad_week_num"],
                        "dekad_text": next(m["dekad_text"] for m in dekad_list if m["id"] == row["dekad_week_num"]),
                        "rainfall": round(row["rainfall"], 1),
                        "mean_rf": round(row["mean_rf"], 1),
                    }
                    for _, row in grouped.iterrows()
                ]
            for n, _ in enumerate(dekad_rf_vals):
                dekad_rf_vals[n]["cumulative_rf"] = round((dekad_rf_vals[n]["rainfall"] if n == 0 else dekad_rf_vals[n]["rainfall"] + dekad_rf_vals[n-1]["cumulative_rf"]), 1)
            dekad_rf_stats = get_curr_common_stats(list(map(lambda x: x["rainfall"], dekad_rf_vals)))
        # 3. rolling past 30 year -- using same dates
        with connect(host=druid["host"], port=druid["port"], path=druid["path"], scheme=druid["scheme"]) as connection:
            hist_rf_vals = []
            for y in range(current_year - 30, current_year):
                smd = "02-28" if is_non_leap_year(y) and start_month_day == "02-29" else start_month_day
                emd = "02-28" if is_non_leap_year(y) and end_month_day == "02-29" else end_month_day
                start_date = f"{y}-{smd}"
                end_date = f"{y}-{emd}"
                query = f"""
                    SELECT "commune_id", "year", ROUND(SUM("grid_rainfall"), 1) AS sum_rf, COUNT(*) AS day_count
                    FROM "druid"."{data_src_table}"
                    WHERE 1=1 AND "{admin_level}_id"='{admin_level_id}'
                    AND "year"={y} 
                    AND "__time">='{start_date}T00:00:00.000Z' 
                    AND "__time"<='{end_date}T00:00:00.000Z'
                    GROUP BY "commune_id", "year"
                """
                query_result = pd.DataFrame(connection.execute(query), dtype=object)
                if not query_result.empty:
                    if admin_level == "commune":
                        row = query_result.iloc[0]
                        hist_rf_vals.append({
                            "year": row["year"],
                            "rainfall": row["sum_rf"] or 0,
                            "day_count": row["day_count"]
                        })
                    else:
                        avg_rf = query_result["sum_rf"].mean()
                        avg_days = query_result["day_count"].mean()   # or first() if same for all
                        hist_rf_vals.append({
                            "year": y,
                            "rainfall": round(avg_rf, 1),
                            "day_count": int(avg_days)
                        })
            hist_rf_avg = (
                round(sum([x["rainfall"] for x in hist_rf_vals]) / len(hist_rf_vals), 1)
                if hist_rf_vals else "N/A"
            )
        # 4. send api response
        return {
            "status": 1, 
            "data": {
                "cy_data": {
                    "cy_start_date": real_start_date,
                    "cy_end_date": real_end_date,
                    "days_count": days_count,
                    "dekad_rf_vals": dekad_rf_vals,
                    "dekad_rf_stats": dekad_rf_stats
                },
                "py_data": {
                    "hist_rf_vals": hist_rf_vals,
                    "hist_rf_avg": hist_rf_avg
                }
            }
        }
    except ProgrammingError:
        return {"status": 0, "message": "Issue with SQL query, please check"}
    except ConnectionError:
        return {"status": 0, "message": f"Couldn't connect to database ({data_src_table}), please try again"}



def get_current_weekly_rainfall(**kwargs):
    admin_level = kwargs.get("admin_level")
    admin_level_id = kwargs.get("admin_level_id") 
    from_week = kwargs.get("from_week")
    to_week = kwargs.get("to_week")
    data_src_table = data_source[kwargs.get('data_src_table')]
    week_filter = f""" AND "met_week_num">={from_week} AND "met_week_num"<={to_week}""" if from_week and to_week else ""
    try:
        # 1. actual available dates in the current year from the selected months
        date_query = f"""
            SELECT MIN("__time"), MAX("__time") FROM "druid"."{data_src_table}"
            WHERE TRUE AND "{admin_level}_id"='{admin_level_id}' {current_year_filter}{week_filter}
        """
        with connect(host=druid["host"], port=druid["port"], path=druid["path"], scheme=druid["scheme"]) as connection:
            date_query_result = pd.DataFrame(connection.execute(date_query), dtype=object).to_records()
            real_start_date = date_query_result[0][1][0:10]
            real_end_date = date_query_result[0][2][0:10]
            days_count = (datetime.strptime(real_end_date, "%Y-%m-%d") - datetime.strptime(real_start_date, "%Y-%m-%d")).days + 1
            start_month_day = real_start_date[5:]
            end_month_day = real_end_date[5:]
        # 2. current year monthly values
        query = f"""
            SELECT "commune_id", "met_week_num", ROUND(SUM("grid_rainfall"), 1) AS "sum", ROUND(AVG("grid_rainfall"), 1) AS "avg"
            FROM "druid"."{data_src_table}"
            WHERE TRUE AND "{admin_level}_id"='{admin_level_id}' {current_year_filter}{week_filter}
            GROUP BY "commune_id", "met_week_num"
        """
        with connect(host=druid["host"], port=druid["port"], path=druid["path"], scheme=druid["scheme"]) as connection:
            query_result = pd.DataFrame(connection.execute(query), dtype=object)
            if admin_level == "commune":
                weekly_rf_vals = [{
                    "week": i[2],
                    "week_text": list(filter(lambda x: x["id"] == i[2], week_list))[0]["week_text"],
                    "rainfall": i[3],
                    "mean_rf": i[4]
                } for i in query_result.to_records()]
            else:
                grouped = (
                    query_result.groupby("met_week_num")
                        .agg(rainfall=("sum", "mean"), mean_rf=("avg", "mean"))
                        .reset_index()
                )
                weekly_rf_vals = [
                    {
                        "week": row["met_week_num"],
                        "week_text": next(m["week_text"] for m in week_list if m["id"] == row["met_week_num"]),
                        "rainfall": round(row["rainfall"], 1),
                        "mean_rf": round(row["mean_rf"], 1),
                    }
                    for _, row in grouped.iterrows()
                ]
            for n, _ in enumerate(weekly_rf_vals):
                weekly_rf_vals[n]["cumulative_rf"] = round((weekly_rf_vals[n]["rainfall"] if n == 0 else weekly_rf_vals[n]["rainfall"] + weekly_rf_vals[n-1]["cumulative_rf"]), 1)
            weekly_rf_stats = get_curr_common_stats(list(map(lambda x: x["rainfall"], weekly_rf_vals)))
        # 3. rolling past 30 year -- using same dates
        with connect(host=druid["host"], port=druid["port"], path=druid["path"], scheme=druid["scheme"]) as connection:
            hist_rf_vals = []
            for y in range(current_year - 30, current_year):
                smd = "02-28" if is_non_leap_year(y) and start_month_day == "02-29" else start_month_day
                emd = "02-28" if is_non_leap_year(y) and end_month_day == "02-29" else end_month_day
                start_date = f"{y}-{smd}"
                end_date = f"{y}-{emd}"
                query = f"""
                    SELECT "commune_id", "year", ROUND(SUM("grid_rainfall"), 1) AS sum_rf, COUNT(*) AS day_count
                    FROM "druid"."{data_src_table}"
                    WHERE 1=1 AND "{admin_level}_id"='{admin_level_id}'
                    AND "year"={y} 
                    AND "__time">='{start_date}T00:00:00.000Z' 
                    AND "__time"<='{end_date}T00:00:00.000Z'
                    GROUP BY "commune_id", "year"
                """
                query_result = pd.DataFrame(connection.execute(query), dtype=object)
                if not query_result.empty:
                    if admin_level == "commune":
                        row = query_result.iloc[0]
                        hist_rf_vals.append({
                            "year": row["year"],
                            "rainfall": row["sum_rf"] or 0,
                            "day_count": row["day_count"]
                        })
                    else:
                        avg_rf = query_result["sum_rf"].mean()
                        avg_days = query_result["day_count"].mean()   # or first() if same for all
                        hist_rf_vals.append({
                            "year": y,
                            "rainfall": round(avg_rf, 1),
                            "day_count": int(avg_days)
                        })
            hist_rf_avg = (
                round(sum([x["rainfall"] for x in hist_rf_vals]) / len(hist_rf_vals), 1)
                if hist_rf_vals else "N/A"
            )
        # 4. send api response
        return {
            "status": 1, 
            "data": {
                "cy_data": {
                    "cy_start_date": real_start_date,
                    "cy_end_date": real_end_date,
                    "days_count": days_count,
                    "week_rf_vals": weekly_rf_vals,
                    "week_rf_stats": weekly_rf_stats
                },
                "py_data": {
                    "hist_rf_vals": hist_rf_vals,
                    "hist_rf_avg": hist_rf_avg
                }
            }
        }
    except ProgrammingError:
        return {"status": 0, "message": "Issue with SQL query, please check"}
    except ConnectionError:
        return {"status": 0, "message": f"Couldn't connect to database ({data_src_table}), please try again"}



def get_current_daily_rainfall(**kwargs):
    admin_level = kwargs.get("admin_level")
    admin_level_id = kwargs.get("admin_level_id") 
    from_date = kwargs.get("from_date")
    to_date = kwargs.get("to_date")
    data_src_table = data_source[kwargs.get('data_src_table')]
    date_filter = f""" AND "__time">='{from_date}T00:00:00.000Z' AND "__time"<='{to_date}T00:00:00.000Z'""" if from_date and to_date else ""
    try:
        # 1. actual available dates in the current year from the selected months
        date_query = f"""
            SELECT MIN("__time"), MAX("__time") FROM "druid"."{data_src_table}"
            WHERE TRUE AND "{admin_level}_id"='{admin_level_id}' {current_year_filter}{date_filter}
        """
        with connect(host=druid["host"], port=druid["port"], path=druid["path"], scheme=druid["scheme"]) as connection:
            date_query_result = pd.DataFrame(connection.execute(date_query), dtype=object).to_records()
            real_start_date = date_query_result[0][1][0:10]
            real_end_date = date_query_result[0][2][0:10]
            days_count = (datetime.strptime(real_end_date, "%Y-%m-%d") - datetime.strptime(real_start_date, "%Y-%m-%d")).days + 1
            start_month_day = real_start_date[5:]
            end_month_day = real_end_date[5:]
        # 2. current year monthly values
        query = f"""
            SELECT "commune_id", "__time" AS "timestamp", ROUND(SUM("grid_rainfall"), 1) AS "sum", ROUND(AVG("grid_rainfall"), 1) AS "avg"
            FROM "druid"."{data_src_table}"
            WHERE TRUE AND "{admin_level}_id"='{admin_level_id}' {current_year_filter}{date_filter}
            GROUP BY "commune_id", "__time"
        """
        with connect(host=druid["host"], port=druid["port"], path=druid["path"], scheme=druid["scheme"]) as connection:
            query_result = pd.DataFrame(connection.execute(query), dtype=object)
            if admin_level == "commune":
                daily_rf_vals = [{
                    "date": i[1][0:10],
                    "day_index": next(x for x in met_week_long if int(i[1][5:7]) == x["month"] and int(i[1][8:10]) == x["day"])["id"],
                    "rainfall": i[3],
                    "mean_rf": i[4]
                } for i in query_result.to_records()]
            else:
                grouped = (
                    query_result.groupby("timestamp")
                        .agg(rainfall=("sum", "mean"), mean_rf=("avg", "mean"))
                        .reset_index()
                )
                daily_rf_vals = [
                    {
                        "date": row["timestamp"][0:10],
                        "day_index": next(x for x in met_week_long if int(row["timestamp"][5:7]) == x["month"] and int(row["timestamp"][8:10]) == x["day"])["id"],
                        "rainfall": round(row["rainfall"], 1),
                        "mean_rf": round(row["mean_rf"], 1),
                    }
                    for _, row in grouped.iterrows()
                ]
            for n, _ in enumerate(daily_rf_vals):
                daily_rf_vals[n]["cumulative_rf"] = round((daily_rf_vals[n]["rainfall"] if n == 0 else daily_rf_vals[n]["rainfall"] + daily_rf_vals[n-1]["cumulative_rf"]), 1)
            daily_rf_stats = get_curr_common_stats(list(map(lambda x: x["rainfall"], daily_rf_vals)))
        # 3. rolling past 30 year -- using same dates
        with connect(host=druid["host"], port=druid["port"], path=druid["path"], scheme=druid["scheme"]) as connection:
            hist_rf_vals = []
            for y in range(current_year - 30, current_year):
                smd = "02-28" if is_non_leap_year(y) and start_month_day == "02-29" else start_month_day
                emd = "02-28" if is_non_leap_year(y) and end_month_day == "02-29" else end_month_day
                start_date = f"{y}-{smd}"
                end_date = f"{y}-{emd}"
                query = f"""
                    SELECT "commune_id", "year", ROUND(SUM("grid_rainfall"), 1) AS sum_rf, COUNT(*) AS day_count
                    FROM "druid"."{data_src_table}"
                    WHERE 1=1 AND "{admin_level}_id"='{admin_level_id}'
                    AND "year"={y} 
                    AND "__time">='{start_date}T00:00:00.000Z' 
                    AND "__time"<='{end_date}T00:00:00.000Z'
                    GROUP BY "commune_id", "year"
                """
                query_result = pd.DataFrame(connection.execute(query), dtype=object)
                if not query_result.empty:
                    if admin_level == "commune":
                        row = query_result.iloc[0]
                        hist_rf_vals.append({
                            "year": row["year"],
                            "rainfall": row["sum_rf"] or 0,
                            "day_count": row["day_count"]
                        })
                    else:
                        avg_rf = query_result["sum_rf"].mean()
                        avg_days = query_result["day_count"].mean()   # or first() if same for all
                        hist_rf_vals.append({
                            "year": y,
                            "rainfall": round(avg_rf, 1),
                            "day_count": int(avg_days)
                        })
            hist_rf_avg = (
                round(sum([x["rainfall"] for x in hist_rf_vals]) / len(hist_rf_vals), 1)
                if hist_rf_vals else "N/A"
            )
        return {
            "status": 1,
            "data": {
                "cy_data": {
                    "cy_start_date": real_start_date,
                    "cy_end_date": real_end_date,
                    "days_count": days_count,
                    "daily_rf_vals": daily_rf_vals,
                    "daily_rf_stats": daily_rf_stats
                },
                "py_data": {
                    "hist_rf_vals": hist_rf_vals,
                    "hist_rf_avg": hist_rf_avg
                }
            }
        }
    except ProgrammingError:
        return {"status": 0, "message": "Issue with SQL query, please check"}
    except ConnectionError:
        return {"status": 0, "message": f"Couldn't connect to database ({data_src_table}), please try again"}