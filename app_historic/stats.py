
from math import sqrt

def rf_range_check(rainfall, rf_gte, rf_lt):
    if rf_gte == None and rf_lt == None:
        return True
    elif rf_gte == 0 and rf_lt == 0:
        return True
    elif isinstance(rf_gte, (int, float)) and rf_lt == None:
        return True if rainfall >= rf_gte else False
    elif rf_gte == None and isinstance(rf_lt, (int, float)):
        return True if rainfall < rf_lt else False
    elif isinstance(rf_gte, (int, float)) and isinstance(rf_lt, (int, float)):
        return True if rf_gte <= rainfall < rf_lt else False


def get_hist_rf_common_stats(rf_array):
    if "year_type_match" in rf_array[0]:
        rf_val_list = list(filter(lambda y: y, list(map(lambda x: x["rainfall"] if x["range_match"] and x["year_type_match"] else 0, rf_array))))
    else:
        rf_val_list = list(filter(lambda y: y, list(map(lambda x: x["rainfall"] if x["range_match"] else 0, rf_array))))
    rf_year_count = len(rf_val_list)
    rf_total = sum(rf_val_list)
    rf_min = min(rf_val_list) if rf_year_count else "N/A"
    rf_max = max(rf_val_list) if rf_year_count else "N/A"
    rf_mean = round(sum(rf_val_list)/rf_year_count, 2) if rf_year_count else "N/A"
    rf_std_dev = round(sqrt(sum([((x - rf_mean) ** 2) for x in rf_val_list]) / rf_year_count), 2) if rf_year_count and isinstance(rf_mean, (int, float)) else "N/A"
    rf_cov = round((rf_std_dev * 100)/rf_mean, 2) if rf_year_count and isinstance(rf_mean, (int, float)) else "N/A"
    return {"count": rf_year_count, "total": rf_total, "min": rf_min, "max": rf_max, "mean": rf_mean, "std_dev": rf_std_dev, "cov": rf_cov}


def get_hist_rf_poe(rf_array):
    # probability of exceedance - ignore range match if needed
    if "year_type_match" in rf_array[0]:
        rf_val_list_enso = list(map(lambda x: x["rainfall"] if x["range_match"] and x["year_type_match"] else 0, rf_array))
        rf_val_list = list(filter(lambda x : x, rf_val_list_enso))
    else:
        rf_val_list_all = list(map(lambda x: x["rainfall"] if x["range_match"] else 0, rf_array))
        rf_val_list = list(filter(lambda x: x, rf_val_list_all))
    count = len(rf_val_list)
    rf_val_list.sort(reverse=True)
    rf_poe_init = []
    for (n, i) in enumerate(rf_val_list):
        rf_poe_init.append({"rainfall": i, "probability": round(((n+1)*100)/count, 2) })
    rf_poe_init2 = sorted(rf_poe_init, key=lambda k: k["rainfall"])
    rf_poe = []
    for n, i in enumerate(rf_poe_init2):
        if n >= 1:
            if(i["rainfall"] == rf_poe_init2[n-1]["rainfall"]):
                i["probability"] = rf_poe_init2[n-1]["probability"]
        rf_poe.append(i)
    return rf_poe


def temp_range_check(temp, temp_gte, temp_lt):
    if temp_gte == None and temp_lt == None:
        return True
    elif isinstance(temp_gte, (int, float)) and temp_lt == None:
        return True if temp >= temp_gte else False
    elif temp_gte == None and isinstance(temp_lt, (int, float)):
        return True if temp < temp_lt else False
    elif isinstance(temp_gte, (int, float)) and isinstance(temp_lt, (int, float)):
        return True if temp_gte <= temp < temp_lt else False


def get_hist_temp_common_stats(temp_array):
    temp_array = [i or 0 for i in temp_array]
    temp_year_count = len(temp_array)
    temp_min = round(min(temp_array), 2) if temp_year_count else "N/A"
    temp_max = round(max(temp_array), 2) if temp_year_count else "N/A"
    temp_mean = round(sum(temp_array)/temp_year_count, 2) if temp_year_count else "N/A"
    temp_std_dev = round(sqrt(sum([((x - temp_mean) ** 2) for x in temp_array]) / temp_year_count),  2) if temp_year_count and isinstance(temp_mean, (int, float)) and temp_mean else "N/A"
    temp_cov = round((temp_std_dev * 100) / temp_mean, 2) if temp_year_count and isinstance(temp_mean, (int, float)) and temp_mean else "N/A"
    return {"count": temp_year_count, "min": temp_min, "max": temp_max, "mean": temp_mean, "std_dev": temp_std_dev, "cov": temp_cov}

def is_non_leap_year(year):
    return bool(year % 4)


def get_hist_rh_common_stats(rh_array):
    rh_array = [i or 0 for i in rh_array]
    rh_year_count = len(rh_array)
    rh_min = round(min(rh_array), 2) if rh_year_count else "N/A"
    rh_max = round(max(rh_array), 2) if rh_year_count else "N/A"
    rh_mean = round(sum(rh_array)/rh_year_count, 2) if rh_year_count else "N/A"
    rh_std_dev = round(sqrt(sum([((x - rh_mean) ** 2) for x in rh_array]) / rh_year_count),  2) if rh_year_count and isinstance(rh_mean, (int, float)) and rh_mean else "N/A"
    rh_cov = round((rh_std_dev * 100) / rh_mean, 2) if rh_year_count and isinstance(rh_mean, (int, float)) and rh_mean else "N/A"
    return {"count": rh_year_count, "min": rh_min, "max": rh_max, "mean": rh_mean, "std_dev": rh_std_dev, "cov": rh_cov}

def get_hist_ws_common_stats(ws_array):
    ws_array = [i or 0 for i in ws_array]
    ws_year_count = len(ws_array)
    ws_min = round(min(ws_array), 2) if ws_year_count else "N/A"
    ws_max = round(max(ws_array), 2) if ws_year_count else "N/A"
    ws_mean = round(sum(ws_array)/ws_year_count, 2) if ws_year_count else "N/A"
    ws_std_dev = round(sqrt(sum([((x - ws_mean) ** 2) for x in ws_array]) / ws_year_count),  2) if ws_year_count and isinstance(ws_mean, (int, float)) and ws_mean else "N/A"
    ws_cov = round((ws_std_dev * 100) / ws_mean, 2) if ws_year_count and isinstance(ws_mean, (int, float)) and ws_mean else "N/A"
    return {"count": ws_year_count, "min": ws_min, "max": ws_max, "mean": ws_mean, "std_dev": ws_std_dev, "cov": ws_cov}