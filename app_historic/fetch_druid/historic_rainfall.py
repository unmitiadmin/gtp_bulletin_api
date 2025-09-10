from pprint import pprint
from dotenv import dotenv_values
from numpy import inner
from pydruid.db import connect
from pydruid.db.exceptions import ProgrammingError
from requests import ConnectionError
import pandas as pd
from django.utils.timezone import now
from app_lookups.constants import week_list, dekad_list, month_list, crop_list
from ..stats import rf_range_check, get_hist_rf_common_stats, get_hist_rf_poe, is_non_leap_year


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
first_year = now().year - int(env["YEARS_AGO_FOR_FIRST_YEAR"])


def get_historic_yearly_rainfall(**kwargs):
    admin_level = kwargs.get('admin_level')
    admin_level_id = kwargs.get('admin_level_id')
    rf_gte = kwargs.get('rf_gte')
    rf_lt = kwargs.get('rf_lt')
    from_week = kwargs.get('from_week')
    to_week = kwargs.get('to_week')
    from_month = kwargs.get('from_month')
    to_month = kwargs.get('to_month')
    from_date = kwargs.get('from_date')
    to_date = kwargs.get('to_date')
    from_dekad = kwargs.get('from_dekad')
    to_dekad = kwargs.get('to_dekad')
    data_src_table = data_source[kwargs.get('data_src_table')]
    year_filter = f""" AND "year" >= {first_year} AND "year" <= {current_year-1}"""
    week_filter = f""" AND "met_week_num">={from_week} AND "met_week_num"<={to_week}""" if from_week and to_week else ""
    month_filter = f""" AND "month_num">={from_month} AND "month_num"<={to_month}""" if from_month and to_month else ""
    date_filter = f""" AND "__time">='{from_date}T00:00:00.000Z' AND "__time"<='{to_date}T00:00:00.000Z'""" if from_date and to_date else ""        
    dekad_filter = f""" AND "dekad_week_num">='{from_dekad}' AND "dekad_week_num"<='{to_dekad}'""" if from_dekad and to_dekad else ""
    try:
        query = f"""
            SELECT "commune_id", "year", "year_type", ROUND(SUM("grid_rainfall"), 0) AS "rainfall"
            FROM "druid"."{data_src_table}"
            WHERE 1=1 {year_filter}
            AND "{admin_level}_id"='{admin_level_id}'
            {month_filter}{dekad_filter}{week_filter}{date_filter}
            GROUP BY "commune_id", "year", "year_type"
        """
        with connect(host=druid["host"], port=druid["port"], path=druid["path"], scheme=druid["scheme"]) as connection:
            df = pd.DataFrame(connection.execute(query), dtype=object)
            if not df.empty:
                if "year" in df.columns:
                    try:
                        df["year"] = df["year"].astype(int)
                    except Exception:
                        pass
                if "rainfall" in df.columns:
                    df["rainfall"] = pd.to_numeric(df["rainfall"], errors="coerce").fillna(0)
            if admin_level == "commune":
                # pick rows for that commune_id only
                cid = int(admin_level_id)
                sel = df[df["commune_id"] == cid] if not df.empty else pd.DataFrame()
                all_year_rf_vals = [
                    {
                        "year": int(row["year"]),
                        "year_type": row["year_type"],
                        "rainfall": int(row["rainfall"] or 0),
                        "range_match": rf_range_check(row["rainfall"], rf_gte, rf_lt),
                    }
                    for _, row in sel.iterrows()
                ]
            else:
                if df.empty:
                    all_year_rf_vals = []
                else:
                    grouped = (
                        df.groupby(["year", "year_type"], as_index=False)
                        .agg(rainfall_mean=("rainfall", "mean"))
                    )
                    all_year_rf_vals = [
                        {
                            "year": int(row["year"]),
                            "year_type": row["year_type"],
                            "rainfall": int(round(row["rainfall_mean"] or 0)),
                            "range_match": rf_range_check(row["rainfall_mean"], rf_gte, rf_lt),
                        }
                        for _, row in grouped.iterrows()
                    ]

            elnino_year_rf_vals = [{"year": i["year"], "year_type_match": True if i["year_type"] == "El Niño" else False,
                                    "rainfall": i["rainfall"], "range_match": rf_range_check(i["rainfall"], rf_gte, rf_lt)
                                    } for i in all_year_rf_vals]
            lanina_year_rf_vals = [{"year": i["year"], "year_type_match": True if i["year_type"] == "La Niña" else False,
                                    "rainfall": i["rainfall"], "range_match": rf_range_check(i["rainfall"], rf_gte, rf_lt)
                                    } for i in all_year_rf_vals]
            # YEARLY STATS
            all_year_rf_stats = get_hist_rf_common_stats(all_year_rf_vals)
            all_years_poe = get_hist_rf_poe(all_year_rf_vals)
            elnino_year_rf_stats = get_hist_rf_common_stats(elnino_year_rf_vals)
            elnino_years_poe = get_hist_rf_poe(elnino_year_rf_vals)
            lanina_year_rf_stats = get_hist_rf_common_stats(lanina_year_rf_vals)
            lanina_years_poe = get_hist_rf_poe(lanina_year_rf_vals)
            data = {
                "all_years_rf_vals": all_year_rf_vals,
                "all_years_rf_stats": all_year_rf_stats,
                "all_years_poe": all_years_poe,
                "elnino_year_rf_vals": elnino_year_rf_vals,
                "elnino_year_rf_stats": elnino_year_rf_stats,
                "elnino_years_poe": elnino_years_poe,
                "lanina_year_rf_vals": lanina_year_rf_vals,
                "lanina_year_rf_stats": lanina_year_rf_stats,
                "lanina_years_poe": lanina_years_poe,
                "total_year_count": len(all_year_rf_vals)
            }
        return {"status": 1, "data": data}
    except ProgrammingError:
        return {"status": 0, "message": "Issue with SQL query, please check"}
    except ConnectionError:
        return {"status": 0, "message": f"Couldn't connect to database ({data_src_table}), please try again"}
    except (IndexError, KeyError):
        return {"status": 0, "message": "Data is unavailable at the source"}


def get_historic_yearly_rainfall_rm(**kwargs):
    admin_level = kwargs.get('admin_level')
    admin_level_id = kwargs.get('admin_level_id')
    rf_gte = kwargs.get('rf_gte')
    rf_lt = kwargs.get('rf_lt')
    from_month = kwargs.get('from_month')
    to_month = kwargs.get('to_month')
    data_src_table = data_source[kwargs.get('data_src_table')]
    try:
        with connect(host=druid["host"], port=druid["port"], path=druid["path"], scheme=druid["scheme"]) as connection:
            all_year_rf_vals = []
            for y in range(first_year, current_year):
                to_month_end = (
                    28 if is_non_leap_year(y + 1) and to_month == 2
                    else next(m for m in month_list if to_month == m["id"])["max_day"]
                )
                query = f"""
                    SELECT "commune_id", "year", "year_type", COUNT(*) as "day_count",
                    ROUND(SUM("grid_rainfall"), 0) AS "rainfall" 
                    FROM "druid"."{data_src_table}" 
                    WHERE 1=1  
                    AND "{admin_level}_id"='{admin_level_id}'
                    AND "__time">='{str(y)}-{str(from_month).zfill(2)}-01T00:00:00.000Z' 
                    AND "__time"<='{str(y+1)}-{str(to_month).zfill(2)}-{str(to_month_end).zfill(2)}T00:00:00.000Z'
                    GROUP BY "commune_id", "year", "year_type"
                """
                df = pd.DataFrame(connection.execute(query), dtype=object)
                if admin_level == "commune":
                    df = df[df["commune_id"] == admin_level_id]
                else:
                    df = df.groupby(["year", "year_type"], as_index=False)["rainfall"].mean()
                if len(df) > 1:
                    # handle seasonal year spans (e.g. 2020–2021)
                    season_result = {
                        "year": f"{df.iloc[0]['year']}-{df.iloc[1]['year']}",
                        "year_type": df.iloc[0]['year_type'],
                        "rainfall": int((df.iloc[0]['rainfall'] or 0) + (df.iloc[1]['rainfall'] or 0)),
                        "range_match": rf_range_check((df.iloc[0]['rainfall'] or 0) + (df.iloc[1]['rainfall'] or 0), rf_gte, rf_lt)
                    }
                    all_year_rf_vals.append(season_result)
            elnino_year_rf_vals = [{"year": i["year"], "year_type_match": True if i["year_type"] == "El Niño" else False,
                                "rainfall": i["rainfall"], "range_match": rf_range_check(i["rainfall"], rf_gte, rf_lt)
                                } for i in all_year_rf_vals]
            lanina_year_rf_vals = [{"year": i["year"], "year_type_match": True if i["year_type"] == "La Niña" else False,
                                    "rainfall": i["rainfall"], "range_match": rf_range_check(i["rainfall"], rf_gte, rf_lt)
                                    } for i in all_year_rf_vals]
            # YEARLY STATS
            all_year_rf_stats = get_hist_rf_common_stats(all_year_rf_vals)
            all_years_poe = get_hist_rf_poe(all_year_rf_vals)
            elnino_year_rf_stats = get_hist_rf_common_stats(elnino_year_rf_vals)
            elnino_years_poe = get_hist_rf_poe(elnino_year_rf_vals)
            lanina_year_rf_stats = get_hist_rf_common_stats(lanina_year_rf_vals)
            lanina_years_poe = get_hist_rf_poe(lanina_year_rf_vals)
            data = {
                "all_years_rf_vals": all_year_rf_vals,
                "all_years_rf_stats": all_year_rf_stats,
                "all_years_poe": all_years_poe,
                "elnino_year_rf_vals": elnino_year_rf_vals,
                "elnino_year_rf_stats": elnino_year_rf_stats,
                "elnino_years_poe": elnino_years_poe,
                "lanina_year_rf_vals": lanina_year_rf_vals,
                "lanina_year_rf_stats": lanina_year_rf_stats,
                "lanina_years_poe": lanina_years_poe,
                "total_year_count": len(all_year_rf_vals)
            }
            return {"status": 1, "data": data}
    except ProgrammingError:
        return {"status": 0, "message": "Issue with SQL query, please check"}
    except ConnectionError:
        return {"status": 0, "message": f"Couldn't connect to database ({data_src_table}), please try again"}
    except (IndexError, KeyError):
        return {"status": 0, "message": "Data is unavailable at the source"}


def get_historic_yearly_rainfall_rd(**kwargs):
    admin_level = kwargs.get('admin_level')
    admin_level_id = kwargs.get('admin_level_id')
    rf_gte = kwargs.get('rf_gte')
    rf_lt = kwargs.get('rf_lt')
    from_dekad = kwargs.get('from_dekad')
    to_dekad = kwargs.get('to_dekad')
    data_src_table = data_source[kwargs.get('data_src_table')]
    try:
        with connect(host=druid["host"], port=druid["port"], path=druid["path"], scheme=druid["scheme"]) as connection:
            all_year_rf_vals = []
            for y in range(first_year, current_year):
                from_dekad_start = next(d for d in dekad_list if d["id"] == from_dekad)
                start_date = f"""{str(from_dekad_start["min_month"]).zfill(2)}-{str(from_dekad_start["min_day"]).zfill(2)}"""
                to_dekad_end_dt = 28 if is_non_leap_year(y+1) and to_dekad == 6 \
                    else next(d for d in dekad_list if to_dekad == d["id"])["max_day"]
                to_dekad_end = next(d for d in dekad_list if d["id"] == to_dekad)
                end_date = f"""{str(to_dekad_end["max_month"]).zfill(2)}-{to_dekad_end_dt}"""
                query = f"""
                    SELECT "commune_id", "year", "year_type", COUNT(*) as "day_count",
                    ROUND(SUM("grid_rainfall"), 0) AS "rainfall"
                    FROM "druid"."{data_src_table}"
                    WHERE 1=1
                    AND "{admin_level}_id"='{admin_level_id}'
                    AND "__time">='{str(y)}-{start_date}T00:00:00.000Z'
                    AND "__time"<='{str(y+1)}-{end_date}T00:00:00.000Z'
                    GROUP BY "commune_id", "year", "year_type"
                """
                df = pd.DataFrame(connection.execute(query), dtype=object)
                if df.empty:
                    continue
                if admin_level == "commune":
                    df = df[df["commune_id"] == admin_level_id]
                else:
                    df = df.groupby(["year", "year_type"], as_index=False)["rainfall"].mean()
                if len(df) > 1:
                    total_rf = df["rainfall"].sum()
                    season_result = {
                        "year": f"{df.iloc[0]['year']}-{df.iloc[1]['year']}",
                        "year_type": df.iloc[0]['year_type'],  # pick one, they should match
                        "rainfall": int(total_rf),
                        "range_match": rf_range_check(total_rf, rf_gte, rf_lt)
                    }
                    all_year_rf_vals.append(season_result)
            elnino_year_rf_vals = [{"year": i["year"], "year_type_match": True if i["year_type"] == "El Niño" else False,
                                    "rainfall": i["rainfall"], "range_match": rf_range_check(i["rainfall"], rf_gte, rf_lt)
                                    } for i in all_year_rf_vals]
            lanina_year_rf_vals = [{"year": i["year"], "year_type_match": True if i["year_type"] == "La Niña" else False,
                                    "rainfall": i["rainfall"], "range_match": rf_range_check(i["rainfall"], rf_gte, rf_lt)
                                    } for i in all_year_rf_vals]
            # YEARLY STATS
            all_year_rf_stats = get_hist_rf_common_stats(all_year_rf_vals)
            all_years_poe = get_hist_rf_poe(all_year_rf_vals)
            elnino_year_rf_stats = get_hist_rf_common_stats(elnino_year_rf_vals)
            elnino_years_poe = get_hist_rf_poe(elnino_year_rf_vals)
            lanina_year_rf_stats = get_hist_rf_common_stats(lanina_year_rf_vals)
            lanina_years_poe = get_hist_rf_poe(lanina_year_rf_vals)
            data = {
                "all_years_rf_vals": all_year_rf_vals,
                "all_years_rf_stats": all_year_rf_stats,
                "all_years_poe": all_years_poe,
                "elnino_year_rf_vals": elnino_year_rf_vals,
                "elnino_year_rf_stats": elnino_year_rf_stats,
                "elnino_years_poe": elnino_years_poe,
                "lanina_year_rf_vals": lanina_year_rf_vals,
                "lanina_year_rf_stats": lanina_year_rf_stats,
                "lanina_years_poe": lanina_years_poe,
                "total_year_count": len(all_year_rf_vals)
            }
            return {"status": 1, "data": data}
    except ProgrammingError:
        return {"status": 0, "message": "Issue with SQL query, please check"}
    except ConnectionError:
        return {"status": 0, "message": f"Couldn't connect to database ({data_src_table}), please try again"}
    except (IndexError, KeyError):
        return {"status": 0, "message": "Data is unavailable at the source"}


def get_historic_yearly_rainfall_rw(**kwargs):
    admin_level = kwargs.get('admin_level')
    admin_level_id = kwargs.get('admin_level_id')
    rf_gte = kwargs.get('rf_gte')
    rf_lt = kwargs.get('rf_lt')
    from_week = kwargs.get('from_week')
    to_week = kwargs.get('to_week')
    data_src_table = data_source[kwargs.get('data_src_table')]
    try:
        with connect(host=druid["host"], port=druid["port"], path=druid["path"], scheme=druid["scheme"]) as connection:
            all_year_rf_vals = []
            for y in range(first_year, current_year):
                from_week_start = next(w for w in week_list if w["id"] == from_week)
                start_date = f"""{str(from_week_start["min_month"]).zfill(2)}-{str(from_week_start["min_day"]).zfill(2)}"""
                to_week_end = next(w for w in week_list if w["id"] == to_week)
                end_date = f"""{str(to_week_end["max_month"]).zfill(2)}-{str(to_week_end["max_day"]).zfill(2)}"""
                query = f"""
                    SELECT "commune_id", "year", "year_type", COUNT(*) as "day_count",
                    ROUND(SUM("grid_rainfall"), 0) AS "rainfall"
                    FROM "druid"."{data_src_table}"
                    WHERE 1=1
                    AND "{admin_level}_id"='{admin_level_id}'
                    AND "__time">='{str(y)}-{start_date}T00:00:00.000Z'
                    AND "__time"<='{str(y+1)}-{end_date}T00:00:00.000Z'
                    GROUP BY "commune_id", "year", "year_type"
                """
                df = pd.DataFrame(connection.execute(query), dtype=object)
                if df.empty:
                    continue
                if admin_level == "commune":
                    df = df[df["commune_id"] == admin_level_id]
                else:
                    df = df.groupby(["year", "year_type"], as_index=False)["rainfall"].mean()
                if len(df) > 1:
                    total_rf = df["rainfall"].sum()
                    season_result = {
                        "year": f"{df.iloc[0]['year']}-{df.iloc[1]['year']}",
                        "year_type": df.iloc[0]['year_type'],  # pick one, they should match
                        "rainfall": int(total_rf),
                        "range_match": rf_range_check(total_rf, rf_gte, rf_lt)
                    }
                    all_year_rf_vals.append(season_result)
            elnino_year_rf_vals = [{"year": i["year"], "year_type_match": True if i["year_type"] == "El Niño" else False,
                                    "rainfall": i["rainfall"], "range_match": rf_range_check(i["rainfall"], rf_gte, rf_lt)
                                    } for i in all_year_rf_vals]
            lanina_year_rf_vals = [{"year": i["year"], "year_type_match": True if i["year_type"] == "La Niña" else False,
                                    "rainfall": i["rainfall"], "range_match": rf_range_check(i["rainfall"], rf_gte, rf_lt)
                                    } for i in all_year_rf_vals]
            # YEARLY STATS
            all_year_rf_stats = get_hist_rf_common_stats(all_year_rf_vals)
            all_years_poe = get_hist_rf_poe(all_year_rf_vals)
            elnino_year_rf_stats = get_hist_rf_common_stats(elnino_year_rf_vals)
            elnino_years_poe = get_hist_rf_poe(elnino_year_rf_vals)
            lanina_year_rf_stats = get_hist_rf_common_stats(lanina_year_rf_vals)
            lanina_years_poe = get_hist_rf_poe(lanina_year_rf_vals)
            data = {
                "all_years_rf_vals": all_year_rf_vals,
                "all_years_rf_stats": all_year_rf_stats,
                "all_years_poe": all_years_poe,
                "elnino_year_rf_vals": elnino_year_rf_vals,
                "elnino_year_rf_stats": elnino_year_rf_stats,
                "elnino_years_poe": elnino_years_poe,
                "lanina_year_rf_vals": lanina_year_rf_vals,
                "lanina_year_rf_stats": lanina_year_rf_stats,
                "lanina_years_poe": lanina_years_poe,
                "total_year_count": len(all_year_rf_vals)
            }
            return {"status": 1, "data": data}
    except ProgrammingError:
        return {"status": 0, "message": "Issue with SQL query, please check"}
    except ConnectionError:
        return {"status": 0, "message": f"Couldn't connect to database ({data_src_table}), please try again"}
    except (IndexError, KeyError):
        return {"status": 0, "message": "Data is unavailable at the source"}


def get_historic_dry_spells(**kwargs):
    admin_level = kwargs.get('admin_level')
    admin_level_id = kwargs.get('admin_level_id')
    rf_lt = kwargs.get('rf_lt')
    from_week = kwargs.get('from_week')
    to_week = kwargs.get('to_week')
    from_dekad = kwargs.get("from_dekad")
    to_dekad = kwargs.get("to_dekad")
    resolution = kwargs.get("resolution")
    year_filter = f""" AND "year" >= {first_year} AND "year" <= {current_year-1}"""
    week_filter = f"""AND "met_week_num">={from_week} AND "met_week_num"<={to_week}""" if from_week and to_week else ""
    dekad_filter = f""" AND "dekad_week_num">={from_dekad} AND "dekad_week_num"<={to_dekad}""" if from_dekad and to_dekad else ""
    resolution_filter = dekad_filter if resolution == "dekad" else week_filter
    group_by_column = "dekad_week_num" if resolution == "dekad" else "met_week_num"
    data_src_table = data_source[kwargs.get('data_src_table')]
    try:
        query = f"""
            SELECT "year", {group_by_column} AS grp, "year_type",
            "commune_id", ROUND(SUM("grid_rainfall"), 1) AS commune_sum
            FROM "druid"."{data_src_table}"
            WHERE 1=1 
            AND "{admin_level}_id" = '{admin_level_id}'
            {year_filter} {resolution_filter}
            GROUP BY "year", {group_by_column}, "year_type", "commune_id"
        """
        with connect(host=druid["host"], port=druid["port"], path=druid["path"], scheme=druid["scheme"]) as connection:
            df = pd.DataFrame(connection.execute(query), dtype=object)
            week_year_rf_vals = []
            n = 0
            if admin_level == "commune":
                for _, row in df.iterrows():
                    n += 1
                    week_year_rf_vals.append({
                        "year": int(row["year"]),
                        "week": int(row["grp"]),
                        "rainfall": float(row["commune_sum"] or 0),
                        "enso": row["year_type"],
                        "range_match": rf_range_check(row["commune_sum"] or 0, rf_gte=None, rf_lt=rf_lt),
                        "week_index": n
                    })
            else:
                grouped = df.groupby(["year", "grp", "year_type"], as_index=False).agg(
                    rainfall_mean=("commune_sum", "mean")
                )
                for _, row in grouped.iterrows():
                    n += 1
                    week_year_rf_vals.append({
                        "year": int(row["year"]),
                        "week": int(row["grp"]),
                        "rainfall": float(row["rainfall_mean"] or 0),
                        "enso": row["year_type"],
                        "range_match": rf_range_check(row["rainfall_mean"] or 0, rf_gte=None, rf_lt=rf_lt),
                        "week_index": n
                    })
            total_years = len(list(set(map(lambda x: x["year"], week_year_rf_vals))))
            week_rf_probabilities = []
            resolution_range = (
                range(from_week, to_week+1) if resolution == "week"
                else range(from_dekad, to_dekad+1)
            )
            for wk in resolution_range:
                matched_years = len(list(filter(lambda x: x["range_match"] and x["week"] == wk, week_year_rf_vals)))
                week_rf_probabilities.append({
                    "week": wk, 
                    "week_text": (
                        next((i for i in dekad_list if i["id"] == wk))["dekad_text"]
                        if resolution == "dekad"
                        else next((i for i in week_list if i["id"] == wk))["week_text"]
                    ),  
                    "probability": round((matched_years*100)/total_years, 2)
                })
            return {"status": 1, "data": {
                "resolution": resolution,
                "week_year_rf_vals": week_year_rf_vals,
                "week_rf_probabilities": week_rf_probabilities
            }}
    except ProgrammingError:
        return {"status": 0, "message": "Issue with SQL query, please check"}
    except ConnectionError:
        return {"status": 0, "message": f"Couldn't connect to database ({data_src_table}), please try again"}
    except (IndexError, KeyError):
        return {"status": 0, "message": "Data is unavailable at the source"}


def get_historic_dry_spells_rd(**kwargs):
    admin_level = kwargs.get('admin_level')
    admin_level_id = kwargs.get('admin_level_id')
    rf_lt = kwargs.get('rf_lt')
    from_dekad = kwargs.get('from_dekad')
    to_dekad = kwargs.get('to_dekad')
    data_src_table = data_source[kwargs.get('data_src_table')]
    try:
        with connect(host=druid["host"], port=druid["port"], path=druid["path"], scheme=druid["scheme"]) as connection:
            week_year_rf_vals = []
            for y in range(first_year, current_year):
                from_dekad_start = next(d for d in dekad_list if d["id"] == from_dekad)
                start_date = f"""{str(from_dekad_start["min_month"]).zfill(2)}-{str(from_dekad_start["min_day"]).zfill(2)}"""
                to_dekad_end_dt = 28 if is_non_leap_year(y+1) and to_dekad == 6 else next(d for d in dekad_list if to_dekad == d["id"])["max_day"]
                to_dekad_end = next(d for d in dekad_list if d["id"] == to_dekad)
                end_date = f"""{str(to_dekad_end["max_month"]).zfill(2)}-{to_dekad_end_dt}"""
                query = f"""
                    SELECT "year", "dekad_week_num", "year_type", "commune_id",
                    ROUND(SUM("grid_rainfall"), 1) AS commune_sum
                    FROM "druid"."{data_src_table}"
                    WHERE 1=1
                    AND "{admin_level}_id" = '{admin_level_id}'
                    AND "__time" >= '{str(y)}-{start_date}T00:00:00.000Z'
                    AND "__time" <= '{str(y+1)}-{end_date}T00:00:00.000Z'
                    GROUP BY "year", "dekad_week_num", "year_type", "commune_id"
                """
                df = pd.DataFrame(connection.execute(query), dtype=object)
                week_year_rf_vals = []
                if admin_level == "commune":
                    n = 0
                    for _, row in df.iterrows():
                        if int(row["dekad_week_num"]) <= 36:
                            n += 1
                            week_year_rf_vals.append({
                                "year": f"{df['year'].iloc[0]}-{df['year'].iloc[-1]}",
                                "week": int(row["dekad_week_num"]),
                                "rainfall": float(row["commune_sum"] or 0),
                                "enso": row["year_type"],
                                "range_match": rf_range_check(row["commune_sum"] or 0, rf_gte=None, rf_lt=rf_lt),
                                "week_index": n
                            })
                else:
                    grouped = df.groupby(["year", "dekad_week_num", "year_type"], as_index=False).agg(
                        rainfall_mean=("commune_sum", "mean")
                    )
                    n = 0
                    for _, row in grouped.iterrows():
                        if int(row["dekad_week_num"]) <= 36:
                            n += 1
                            week_year_rf_vals.append({
                                "year": f"{grouped['year'].iloc[0]}-{grouped['year'].iloc[-1]}",
                                "week": int(row["dekad_week_num"]),
                                "rainfall": float(row["rainfall_mean"] or 0),
                                "enso": row["year_type"],
                                "range_match": rf_range_check(row["rainfall_mean"] or 0, rf_gte=None, rf_lt=rf_lt),
                                "week_index": n
                            })
            total_years = len(list(set(map(lambda x: x["year"], week_year_rf_vals))))
            week_nums = list(set(map(lambda x: x["week"], week_year_rf_vals)))
            week_nums = list(filter(lambda x: from_dekad <= x <= 36, week_nums)) + list(filter(lambda x: 1 <= x <= to_dekad, week_nums))
            week_rf_probabilities = []
            for wk in week_nums:
                if wk <= 36:
                    matched_years = len(list(filter(lambda x: x["range_match"] and x["week"] == wk, week_year_rf_vals)))
                    week_rf_probabilities.append({
                        "week": wk, 
                        "week_text": next(i for i in dekad_list if i["id"] == wk)["dekad_text"],  
                        "probability": round((matched_years*100)/total_years, 2)
                    })
            data = {
                "resolution": "dekad",
                "week_year_rf_vals": week_year_rf_vals,
                "week_rf_probabilities": week_rf_probabilities
            }
            return {"status": 1, "data": data}
    except ProgrammingError:
        return {"status": 0, "message": "Data is unavailable at the source"}
    except ConnectionError:
        return {"status": 0, "message": f"Couldn't connect to database ({data_src_table}), please try again"}
    except (IndexError, KeyError):
        return {"status": 0, "message": "Data is unavailable at the source"}


def get_historic_dry_spells_rw(**kwargs):
    admin_level = kwargs.get('admin_level')
    admin_level_id = kwargs.get('admin_level_id')
    rf_lt = kwargs.get('rf_lt')
    from_week = kwargs.get('from_week')
    to_week = kwargs.get('to_week')
    data_src_table = data_source[kwargs.get('data_src_table')]
    try:
        with connect(host=druid["host"], port=druid["port"], path=druid["path"], scheme=druid["scheme"]) as connection:
            week_year_rf_vals = []
            for y in range(first_year, current_year):
                from_week_start = next(w for w in week_list if w["id"] == from_week)
                start_date = f"""{str(from_week_start["min_month"]).zfill(2)}-{str(from_week_start["min_day"]).zfill(2)}"""
                to_week_end = next(w for w in week_list if w["id"] == to_week)
                end_date = f"""{str(to_week_end["max_month"]).zfill(2)}-{str(to_week_end["max_day"]).zfill(2)}"""
                query = f"""
                    SELECT "year", "met_week_num", "year_type", "commune_id",
                    ROUND(SUM("grid_rainfall"), 1) AS commune_sum
                    FROM "druid"."{data_src_table}"
                    WHERE 1=1
                    AND "{admin_level}_id" = '{admin_level_id}'
                    AND "__time" >= '{str(y)}-{start_date}T00:00:00.000Z'
                    AND "__time" <= '{str(y+1)}-{end_date}T00:00:00.000Z'
                    GROUP BY "year", "met_week_num", "year_type", "commune_id"
                """
                df = pd.DataFrame(connection.execute(query), dtype=object)
                week_year_rf_vals = []
                if admin_level == "commune":
                    n = 0
                    for _, row in df.iterrows():
                        if int(row["met_week_num"]) <= 52:
                            n += 1
                            week_year_rf_vals.append({
                                "year": f"{df['year'].iloc[0]}-{df['year'].iloc[-1]}",
                                "week": int(row["met_week_num"]),
                                "rainfall": float(row["commune_sum"] or 0),
                                "enso": row["year_type"],
                                "range_match": rf_range_check(row["commune_sum"] or 0, rf_gte=None, rf_lt=rf_lt),
                                "week_index": n
                            })
                else:
                    grouped = df.groupby(["year", "met_week_num", "year_type"], as_index=False).agg(
                        rainfall_mean=("commune_sum", "mean")
                    )
                    n = 0
                    for _, row in grouped.iterrows():
                        if int(row["met_week_num"]) <= 52:
                            n += 1
                            week_year_rf_vals.append({
                                "year": f"{grouped['year'].iloc[0]}-{grouped['year'].iloc[-1]}",
                                "week": int(row["met_week_num"]),
                                "rainfall": float(row["rainfall_mean"] or 0),
                                "enso": row["year_type"],
                                "range_match": rf_range_check(row["rainfall_mean"] or 0, rf_gte=None, rf_lt=rf_lt),
                                "week_index": n
                            })
            total_years = len(list(set(map(lambda x: x["year"], week_year_rf_vals))))
            week_nums = list(set(map(lambda x: x["week"], week_year_rf_vals)))
            week_nums = list(filter(lambda x: from_week <= x <= 52, week_nums)) + list(filter(lambda x: 1 <= x <= to_week, week_nums))
            week_rf_probabilities = []
            for wk in week_nums:
                if wk <= 52:
                    matched_years = len(list(filter(lambda x: x["range_match"] and x["week"] == wk, week_year_rf_vals)))
                    week_rf_probabilities.append({
                        "week": wk, 
                        "week_text": next(i for i in week_list if i["id"] == wk)["week_text"],  
                        "probability": round((matched_years*100)/total_years, 2)
                    })
            data = {
                "resolution": "week",
                "week_year_rf_vals": week_year_rf_vals,
                "week_rf_probabilities": week_rf_probabilities
            }
            return {"status": 1, "data": data}
    except ProgrammingError:
        return {"status": 0, "message": "Data is unavailable at the source"}
    except ConnectionError:
        return {"status": 0, "message": f"Couldn't connect to database ({data_src_table}), please try again"}
    except (IndexError, KeyError):
        return {"status": 0, "message": "Data is unavailable at the source"}


def get_historic_wet_spells(**kwargs):
    admin_level = kwargs.get('admin_level')
    admin_level_id = kwargs.get('admin_level_id')
    rf_gte = kwargs.get('rf_gte')
    from_week = kwargs.get('from_week')
    to_week = kwargs.get('to_week')
    from_dekad = kwargs.get("from_dekad")
    to_dekad = kwargs.get("to_dekad")
    resolution = kwargs.get("resolution")
    year_filter = f""" AND "year" >= {first_year} AND "year" <= {current_year-1}"""
    week_filter = f"""AND "met_week_num">={from_week} AND "met_week_num"<={to_week}""" if from_week and to_week else ""
    dekad_filter = f""" AND "dekad_week_num">={from_dekad} AND "dekad_week_num"<={to_dekad}""" if from_dekad and to_dekad else ""
    resolution_filter = dekad_filter if resolution == "dekad" else week_filter
    group_by_column = "dekad_week_num" if resolution == "dekad" else "met_week_num"
    data_src_table = data_source[kwargs.get('data_src_table')]
    try:
        query = f"""
            SELECT "year", {group_by_column} AS grp, "year_type",
            "commune_id", ROUND(SUM("grid_rainfall"), 1) AS commune_sum
            FROM "druid"."{data_src_table}"
            WHERE 1=1 
            AND "{admin_level}_id" = '{admin_level_id}'
            {year_filter} {resolution_filter}
            GROUP BY "year", {group_by_column}, "year_type", "commune_id"
        """
        with connect(host=druid["host"], port=druid["port"], path=druid["path"], scheme=druid["scheme"]) as connection:
            df = pd.DataFrame(connection.execute(query), dtype=object)
            week_year_rf_vals = []
            n = 0
            if admin_level == "commune":
                for _, row in df.iterrows():
                    n += 1
                    week_year_rf_vals.append({
                        "year": int(row["year"]),
                        "week": int(row["grp"]),
                        "rainfall": float(row["commune_sum"] or 0),
                        "enso": row["year_type"],
                        "range_match": rf_range_check(row["commune_sum"] or 0, rf_gte=rf_gte, rf_lt=None),
                        "week_index": n
                    })
            else:
                grouped = df.groupby(["year", "grp", "year_type"], as_index=False).agg(
                    rainfall_mean=("commune_sum", "mean")
                )
                for _, row in grouped.iterrows():
                    n += 1
                    week_year_rf_vals.append({
                        "year": int(row["year"]),
                        "week": int(row["grp"]),
                        "rainfall": float(row["rainfall_mean"] or 0),
                        "enso": row["year_type"],
                        "range_match": rf_range_check(row["rainfall_mean"] or 0, rf_gte=rf_gte, rf_lt=None),
                        "week_index": n
                    })
            total_years = len(list(set(map(lambda x: x["year"], week_year_rf_vals))))
            week_rf_probabilities = []
            resolution_range = (
                range(from_week, to_week+1) if resolution == "week"
                else range(from_dekad, to_dekad+1)
            )
            for wk in resolution_range:
                matched_years = len(list(filter(lambda x: x["range_match"] and x["week"] == wk, week_year_rf_vals)))
                week_rf_probabilities.append({
                    "week": wk, 
                    "week_text": (
                        next((i for i in dekad_list if i["id"] == wk))["dekad_text"]
                        if resolution == "dekad"
                        else next((i for i in week_list if i["id"] == wk))["week_text"]
                    ),  
                    "probability": round((matched_years*100)/total_years, 2)
                })
            return {"status": 1, "data": {
                "resolution": resolution,
                "week_year_rf_vals": week_year_rf_vals,
                "week_rf_probabilities": week_rf_probabilities
            }}
    except ProgrammingError:
        return {"status": 0, "message": "Issue with SQL query, please check"}
    except ConnectionError:
        return {"status": 0, "message": f"Couldn't connect to database ({data_src_table}), please try again"}
    except (IndexError, KeyError):
        return {"status": 0, "message": "Data is unavailable at the source"}


def get_historic_wet_spells_rd(**kwargs):
    admin_level = kwargs.get('admin_level')
    admin_level_id = kwargs.get('admin_level_id')
    rf_gte = kwargs.get('rf_gte')
    from_dekad = kwargs.get('from_dekad')
    to_dekad = kwargs.get('to_dekad')
    data_src_table = data_source[kwargs.get('data_src_table')]
    try:
        with connect(host=druid["host"], port=druid["port"], path=druid["path"], scheme=druid["scheme"]) as connection:
            week_year_rf_vals = []
            for y in range(first_year, current_year):
                from_dekad_start = next(d for d in dekad_list if d["id"] == from_dekad)
                start_date = f"""{str(from_dekad_start["min_month"]).zfill(2)}-{str(from_dekad_start["min_day"]).zfill(2)}"""
                to_dekad_end_dt = 28 if is_non_leap_year(y+1) and to_dekad == 6 else next(d for d in dekad_list if to_dekad == d["id"])["max_day"]
                to_dekad_end = next(d for d in dekad_list if d["id"] == to_dekad)
                end_date = f"""{str(to_dekad_end["max_month"]).zfill(2)}-{to_dekad_end_dt}"""
                query = f"""
                    SELECT "year", "dekad_week_num", "year_type", "commune_id",
                    ROUND(SUM("grid_rainfall"), 1) AS commune_sum
                    FROM "druid"."{data_src_table}"
                    WHERE 1=1
                    AND "{admin_level}_id" = '{admin_level_id}'
                    AND "__time" >= '{str(y)}-{start_date}T00:00:00.000Z'
                    AND "__time" <= '{str(y+1)}-{end_date}T00:00:00.000Z'
                    GROUP BY "year", "dekad_week_num", "year_type", "commune_id"
                """
                df = pd.DataFrame(connection.execute(query), dtype=object)
                week_year_rf_vals = []
                if admin_level == "commune":
                    n = 0
                    for _, row in df.iterrows():
                        if int(row["dekad_week_num"]) <= 36:
                            n += 1
                            week_year_rf_vals.append({
                                "year": f"{df['year'].iloc[0]}-{df['year'].iloc[-1]}",
                                "week": int(row["dekad_week_num"]),
                                "rainfall": float(row["commune_sum"] or 0),
                                "enso": row["year_type"],
                                "range_match": rf_range_check(row["commune_sum"] or 0, rf_gte=rf_gte, rf_lt=None),
                                "week_index": n
                            })
                else:
                    grouped = df.groupby(["year", "dekad_week_num", "year_type"], as_index=False).agg(
                        rainfall_mean=("commune_sum", "mean")
                    )
                    n = 0
                    for _, row in grouped.iterrows():
                        if int(row["dekad_week_num"]) <= 36:
                            n += 1
                            week_year_rf_vals.append({
                                "year": f"{grouped['year'].iloc[0]}-{grouped['year'].iloc[-1]}",
                                "week": int(row["dekad_week_num"]),
                                "rainfall": float(row["rainfall_mean"] or 0),
                                "enso": row["year_type"],
                                "range_match": rf_range_check(row["rainfall_mean"] or 0, rf_gte=rf_gte, rf_lt=None),
                                "week_index": n
                            })
            total_years = len(list(set(map(lambda x: x["year"], week_year_rf_vals))))
            week_nums = list(set(map(lambda x: x["week"], week_year_rf_vals)))
            week_nums = list(filter(lambda x: from_dekad <= x <= 36, week_nums)) + list(filter(lambda x: 1 <= x <= to_dekad, week_nums))
            week_rf_probabilities = []
            for wk in week_nums:
                if wk <= 36:
                    matched_years = len(list(filter(lambda x: x["range_match"] and x["week"] == wk, week_year_rf_vals)))
                    week_rf_probabilities.append({
                        "week": wk, 
                        "week_text": next(i for i in dekad_list if i["id"] == wk)["dekad_text"],  
                        "probability": round((matched_years*100)/total_years, 2)
                    })
            data = {
                "resolution": "dekad",
                "week_year_rf_vals": week_year_rf_vals,
                "week_rf_probabilities": week_rf_probabilities
            }
            return {"status": 1, "data": data}
    except ProgrammingError:
        return {"status": 0, "message": "Data is unavailable at the source"}
    except ConnectionError:
        return {"status": 0, "message": f"Couldn't connect to database ({data_src_table}), please try again"}
    except (IndexError, KeyError):
        return {"status": 0, "message": "Data is unavailable at the source"}


def get_historic_wet_spells_rw(**kwargs):
    admin_level = kwargs.get('admin_level')
    admin_level_id = kwargs.get('admin_level_id')
    rf_gte = kwargs.get('rf_gte')
    from_week = kwargs.get('from_week')
    to_week = kwargs.get('to_week')
    data_src_table = data_source[kwargs.get('data_src_table')]
    try:
        with connect(host=druid["host"], port=druid["port"], path=druid["path"], scheme=druid["scheme"]) as connection:
            week_year_rf_vals = []
            for y in range(first_year, current_year):
                from_week_start = next(w for w in week_list if w["id"] == from_week)
                start_date = f"""{str(from_week_start["min_month"]).zfill(2)}-{str(from_week_start["min_day"]).zfill(2)}"""
                to_week_end = next(w for w in week_list if w["id"] == to_week)
                end_date = f"""{str(to_week_end["max_month"]).zfill(2)}-{str(to_week_end["max_day"]).zfill(2)}"""
                query = f"""
                    SELECT "year", "met_week_num", "year_type", "commune_id",
                    ROUND(SUM("grid_rainfall"), 1) AS commune_sum
                    FROM "druid"."{data_src_table}"
                    WHERE 1=1
                    AND "{admin_level}_id" = '{admin_level_id}'
                    AND "__time" >= '{str(y)}-{start_date}T00:00:00.000Z'
                    AND "__time" <= '{str(y+1)}-{end_date}T00:00:00.000Z'
                    GROUP BY "year", "met_week_num", "year_type", "commune_id"
                """
                df = pd.DataFrame(connection.execute(query), dtype=object)
                week_year_rf_vals = []
                if admin_level == "commune":
                    n = 0
                    for _, row in df.iterrows():
                        if int(row["met_week_num"]) <= 52:
                            n += 1
                            week_year_rf_vals.append({
                                "year": f"{df['year'].iloc[0]}-{df['year'].iloc[-1]}",
                                "week": int(row["met_week_num"]),
                                "rainfall": float(row["commune_sum"] or 0),
                                "enso": row["year_type"],
                                "range_match": rf_range_check(row["commune_sum"] or 0, rf_gte=rf_gte, rf_lt=None),
                                "week_index": n
                            })
                else:
                    grouped = df.groupby(["year", "met_week_num", "year_type"], as_index=False).agg(
                        rainfall_mean=("commune_sum", "mean")
                    )
                    n = 0
                    for _, row in grouped.iterrows():
                        if int(row["met_week_num"]) <= 52:
                            n += 1
                            week_year_rf_vals.append({
                                "year": f"{grouped['year'].iloc[0]}-{grouped['year'].iloc[-1]}",
                                "week": int(row["met_week_num"]),
                                "rainfall": float(row["rainfall_mean"] or 0),
                                "enso": row["year_type"],
                                "range_match": rf_range_check(row["rainfall_mean"] or 0, rf_gte=rf_gte, rf_lt=None),
                                "week_index": n
                            })
            total_years = len(list(set(map(lambda x: x["year"], week_year_rf_vals))))
            week_nums = list(set(map(lambda x: x["week"], week_year_rf_vals)))
            week_nums = list(filter(lambda x: from_week <= x <= 52, week_nums)) + list(filter(lambda x: 1 <= x <= to_week, week_nums))
            week_rf_probabilities = []
            for wk in week_nums:
                if wk <= 52:
                    matched_years = len(list(filter(lambda x: x["range_match"] and x["week"] == wk, week_year_rf_vals)))
                    week_rf_probabilities.append({
                        "week": wk, 
                        "week_text": next(i for i in week_list if i["id"] == wk)["week_text"],  
                        "probability": round((matched_years*100)/total_years, 2)
                    })
            data = {
                "resolution": "week",
                "week_year_rf_vals": week_year_rf_vals,
                "week_rf_probabilities": week_rf_probabilities
            }
            return {"status": 1, "data": data}
    except ProgrammingError:
        return {"status": 0, "message": "Data is unavailable at the source"}
    except ConnectionError:
        return {"status": 0, "message": f"Couldn't connect to database ({data_src_table}), please try again"}
    except (IndexError, KeyError):
        return {"status": 0, "message": "Data is unavailable at the source"}


def get_historic_crop_stress(**kwargs):
    admin_level = kwargs.get('admin_level')
    admin_level_id = kwargs.get('admin_level_id')
    crop_id = kwargs.get('crop_id')
    rf_lt = kwargs.get('rf_lt')
    from_week = kwargs.get('from_week')
    data_src_table = data_source[kwargs.get('data_src_table')]
    try:            
        query = f"""
            SELECT "year", "met_week_num", "commune_id", ROUND(SUM("grid_rainfall"), 1) AS commune_sum
            FROM "druid"."{data_src_table}"
            WHERE 1=1 
            AND "year" >= {current_year - 30} 
            AND "year" <= {current_year - 1}
            AND "{admin_level}_id" = '{admin_level_id}'
            GROUP BY "year", "met_week_num", "commune_id"
        """
        with connect(host=druid["host"], port=druid["port"], path=druid["path"], scheme=druid["scheme"]) as connection:
            df = pd.DataFrame(connection.execute(query), dtype=object)
            week_year_rf_vals = []
            if admin_level == "commune":
                for _, row in df.iterrows():
                    week_year_rf_vals.append({
                        "year": int(row["year"]),
                        "week": int(row["met_week_num"]),
                        "rainfall": float(row["commune_sum"] or 0),
                        "range_match": rf_range_check(row["commune_sum"] or 0, rf_gte=None, rf_lt=rf_lt),
                    })
            else:
                grouped = df.groupby(["year", "met_week_num"], as_index=False).agg(
                    rainfall_mean=("commune_sum", "mean")
                )
                for _, row in grouped.iterrows():
                    week_year_rf_vals.append({
                        "year": int(row["year"]),
                        "week": int(row["met_week_num"]),
                        "rainfall": float(row["rainfall_mean"] or 0),
                        "range_match": rf_range_check(row["rainfall_mean"] or 0, rf_gte=None, rf_lt=rf_lt),
                    })
            total_years = len(list(set(map(lambda x: x["year"], week_year_rf_vals))))
            all_week_rf_probabilities = []
            for wk in range(1, 53):
                matched_years = len(list(filter(lambda x: x["range_match"] and x["week"] == wk, week_year_rf_vals)))
                all_week_rf_probabilities.append({"week": wk, "week_text": next((i for i in week_list if i["id"] == wk))["week_text"],
                "probability": round((matched_years * 100) / total_years, 2)})

            crop_obj = list(filter(lambda x: x["id"] == crop_id, crop_list))[0]
            sowing_weeks = crop_obj["sowing_weeks"]
            week_threshold = crop_obj["threshold_percentage"]
            sowing_stages = crop_obj["sowing_stages"]
            week_rf_probabilities = list(filter(lambda x: from_week <= x["week"] < from_week+sowing_weeks, all_week_rf_probabilities))
            if len(week_rf_probabilities) < 15:
                week_rf_probabilities += list(filter(lambda x: x["week"] <= sowing_weeks-len(week_rf_probabilities), all_week_rf_probabilities))
            n = 1
            for i in week_rf_probabilities:
                i["week_index"] = n
                i["prob_match"] = True if i["probability"] >= week_threshold else False
                n += 1
            for stage in sowing_stages:
                for prob in week_rf_probabilities:
                    if prob["week_index"] in range(stage["from"], stage["to"]+1):
                        prob["stage"] = stage["stage"]
            data = {"week_rf_probabilities": week_rf_probabilities, "threshold": week_threshold}
            return {"status": 1, "data": data}
    except ProgrammingError:
        return {"status": 0, "message": "Data is unavailable at the source"}
    except ConnectionError:
        return {"status": 0, "message": f"Couldn't connect to {data_src_table} database"}
    except (IndexError, ZeroDivisionError):
        return {"status": 0, "message": "Data is unavailable at the source"}