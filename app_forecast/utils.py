from pprint import pprint
from math import isnan
from datetime import datetime, timedelta

def to_numeric(val):
    if val and isinstance(val, (int, float)): return val
    if val and isinstance(val, str) and not isnan(eval(val)): return eval(val)
    return None


def get_cardinal_direction(degrees):
    direction_mapping = {
        (348.75, 11.25): "North",
        (11.25, 33.75): "North-NorthEast",
        (33.75, 56.25): "NorthEast",
        (56.25, 78.75): "East-NorthEast",
        (78.75, 101.25): "East",
        (101.25, 123.75): "East-SouthEast",
        (123.75, 146.25): "SouthEast",
        (146.25, 168.75): "South-SouthEast",
        (168.75, 191.25): "South",
        (191.25, 213.75): "South-SouthWest",
        (213.75, 236.25): "SouthWest",
        (236.25, 258.75): "West-SouthWest",
        (258.75, 281.25): "West",
        (281.25, 303.75): "West-NorthWest",
        (303.75, 326.25): "NorthWest",
        (326.25, 348.75): "North-NorthWest"
    }
    
    for key, value in direction_mapping.items():
        if key[0] <= degrees < key[1]:
            return value
    
    # Return a default value if no direction is found
    return None


def get_next_n_months(n):
    current_date = datetime.now()
    if current_date.day > 16:
        current_date = current_date.replace(day=1) + timedelta(days=32)
    months = [current_date.strftime('%b %Y')]
    for _ in range(n - 1):
        current_date = current_date.replace(day=1) + timedelta(days=32)
        months.append(current_date.strftime('%b %Y'))
    return months


def extract_dates(date_string):
    dates = date_string.split(" - ")
    from_date = datetime.strptime(dates[0], "%Y-%m-%d %H:%M:%S").date()
    to_date = datetime.strptime(dates[1], "%Y-%m-%d %H:%M:%S").date()
    return [from_date, to_date]

# rainfall
def rf_gfs(**kwargs):
    referred_date = kwargs.get("referred_date")
    query_result = kwargs.get("query_result")
    result = {
        "referred_date": referred_date,
        "forecast" : [
            {"index": "f00", "date": (datetime.strptime(referred_date, "%Y-%m-%d") + timedelta(days=0)).strftime("%Y-%m-%d"), "rainfall": query_result[0][2]},
            {"index": "f01", "date": (datetime.strptime(referred_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d"), "rainfall": query_result[0][3]},
            {"index": "f02", "date": (datetime.strptime(referred_date, "%Y-%m-%d") + timedelta(days=2)).strftime("%Y-%m-%d"), "rainfall": query_result[0][4]},
            {"index": "f03", "date": (datetime.strptime(referred_date, "%Y-%m-%d") + timedelta(days=3)).strftime("%Y-%m-%d"), "rainfall": query_result[0][5]},
            {"index": "f04", "date": (datetime.strptime(referred_date, "%Y-%m-%d") + timedelta(days=4)).strftime("%Y-%m-%d"), "rainfall": query_result[0][6]},
            {"index": "f05", "date": (datetime.strptime(referred_date, "%Y-%m-%d") + timedelta(days=5)).strftime("%Y-%m-%d"), "rainfall": query_result[0][7]},
            {"index": "f06", "date": (datetime.strptime(referred_date, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d"), "rainfall": query_result[0][8]},
            {"index": "f07", "date": (datetime.strptime(referred_date, "%Y-%m-%d") + timedelta(days=7)).strftime("%Y-%m-%d"), "rainfall": query_result[0][9]},
            {"index": "f08", "date": (datetime.strptime(referred_date, "%Y-%m-%d") + timedelta(days=8)).strftime("%Y-%m-%d"), "rainfall": query_result[0][10]},
            {"index": "f09", "date": (datetime.strptime(referred_date, "%Y-%m-%d") + timedelta(days=9)).strftime("%Y-%m-%d"), "rainfall": query_result[0][11]},
            {"index": "f10", "date": (datetime.strptime(referred_date, "%Y-%m-%d") + timedelta(days=10)).strftime("%Y-%m-%d"), "rainfall": query_result[0][12]},
            {"index": "f11", "date": (datetime.strptime(referred_date, "%Y-%m-%d") + timedelta(days=11)).strftime("%Y-%m-%d"), "rainfall": query_result[0][13]},
            {"index": "f12", "date": (datetime.strptime(referred_date, "%Y-%m-%d") + timedelta(days=12)).strftime("%Y-%m-%d"), "rainfall": query_result[0][14]},
            {"index": "f13", "date": (datetime.strptime(referred_date, "%Y-%m-%d") + timedelta(days=13)).strftime("%Y-%m-%d"), "rainfall": query_result[0][15]},
        ]
    }
    return result


def rf_anacim(**kwargs):
    query_result = kwargs.get("query_result")
    result = []
    for i in query_result:
        result.append({
            "date": i[1][0:10],
            "rainfall": i[2]
        })
    sorted_result = sorted(result, key=lambda x: x.get("date"), reverse=False)
    for n, i in enumerate(sorted_result): i["index"] = f"f{str(n).zfill(2)}"
    return sorted_result


def rf_iri(**kwargs):
    query_result = kwargs.get("query_result")
    result = []
    for i in query_result:
        [start_date, end_date] = extract_dates(i[1])
        result.append({
            "date": f"{start_date} to {end_date}",
            "rainfall": i[2]
        })
    sorted_result = sorted(result, key=lambda x: datetime.strptime(x.get("date")[:10], "%Y-%m-%d"), reverse=False)
    for n, i in enumerate(sorted_result): i["index"] = f"f{str(n).zfill(2)}"
    return sorted_result


def rf_iri_seasonal(**kwargs):
    query_result = kwargs.get("query_result")
    result = []
    for i in query_result:
        result.append({
            "timestamp": i[1][:10],
            "forecast_period": i[2],
            "prob": i[3],
            "tertiary_class": i[4]
        })
    classes = ["Below Normal", "Near Normal", "Above Normal"]
    months_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    uq_period = list(set(map(lambda x: x.get("forecast_period"), result)))
    sorted_uq_period = sorted(uq_period, key=lambda x: months_order.index(x[:3]))
    sorted_result = []
    for class_ in classes:
        for n, period_ in enumerate(sorted_uq_period):
            class_month = list(filter(lambda x: 
                                      x.get("tertiary_class") == class_ 
                                      and x.get("forecast_period") == period_
                                , result))
            if class_month:  
                latest_entry = max(class_month, key=lambda x: datetime.strptime(x["timestamp"], "%Y-%m-%d"))
                sorted_result.append(latest_entry)
            else:
                sorted_result.append({
                    "timestamp": datetime.today().strftime("%Y-%m-%d"),
                    "forecast_period": period_,
                    "prob": 0,
                    "tertiary_class": class_
                })
    return sorted_result


# def parse_date_range(date_range):
#     months = {
#         'January': 1, 'February': 2, 'March': 3, 'April': 4,
#         'May': 5, 'June': 6, 'July': 7, 'August': 8,
#         'September': 9, 'October': 10, 'November': 11, 'December': 12,
#         'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,
#         'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,
#         'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
#     }
#     # Split the date range on the hyphen
#     start_str, end_str = date_range.split('-')
#     # Extract month and day from the start date string
#     start_parts = start_str.split()
#     start_month = start_parts[0]
#     start_day = int(start_parts[1])
#     # Check if the end month is provided or same as the start month
#     if ' ' in end_str:
#         end_parts = end_str.split()
#         end_month = end_parts[0]
#         end_day = int(end_parts[1])
#     else:
#         end_month = start_month
#         end_day = int(end_str)
#     # Determine the year for end date
#     current_year = datetime.now().year  # Adjust based on your context
#     start_date = datetime(current_year, months[start_month], start_day)
#     # If end month is before start month, it indicates a transition to the next year
#     end_date = datetime(current_year + 1, months[end_month], end_day) \
#         if months[end_month] < months[start_month] \
#         else datetime(current_year, months[end_month], end_day)
#     return start_date, end_date


# "__time", "tertiary_class", "week", "probabilistic", "deterministic"
def rf_anacim_subseasonal(**kwargs):
    class_mapping = {
        "1": "Below Normal",
        "2": "Near Normal",
        "3": "Above Normal",
        "1.0": "Below Normal",
        "2.0": "Near Normal",
        "3.0": "Above Normal",
    }
    query_result = kwargs.get("query_result")
    result = []
    if len(query_result):
        initial_result = []
        timestamp = query_result[0][1][:10] if len(query_result) else None
        for i in query_result:
            if " to " in i[3]:
                start_date, end_date = str(i[3]).split(" to ")
                datediff = (datetime.strptime(end_date, "%Y-%m-%d") - datetime.strptime(start_date, "%Y-%m-%d")).days
                if datediff <= 7:
                    initial_result.append({
                        "timestamp": timestamp,
                        "forecast_period": i[3],
                        "start_date": datetime.strptime(start_date, "%Y-%m-%d"),   
                        "prob": i[4]   ,
                        "tertiary_class": class_mapping[i[2]],
                        "forecast_rain": i[5],        
                    })
                    result = sorted(initial_result, key=lambda x: x.get("start_date"))
            else:
                initial_result.append({
                    "timestamp": timestamp,
                    "forecast_period": i[3],
                    "start_date": None,   
                    "prob": i[4]   ,
                    "tertiary_class": class_mapping[i[2]],
                    "forecast_rain": i[5],     
                })
                result = list(filter(lambda x: x.get("forecast_period") in ["Week 1", "Week 2", "Week 3", "Week 4"], initial_result))
                result = sorted(result, key=lambda x: x.get("forecast_period"))
    return result



def rf_anacim_seasonal(**kwargs):
    class_mapping = {
        "1": "Below Normal",
        "2": "Near Normal",
        "3": "Above Normal",
        "1.0": "Below Normal",
        "2.0": "Near Normal",
        "3.0": "Above Normal",
    }
    period_mapping = {
        "January-February-March": 1,
        "February-March-April": 2,
        "March-April-May": 3,
        "April-May-June": 4,
        "May-June-July": 5,
        "June-July-August": 6,
        "July-August-September": 7,
        "August-September-October": 8,
        "September-October-November": 9,
        "October-November-December": 10,
        "November-December-January": 11,
        "December-January-February": 12,
    }
    query_result = kwargs.get("query_result")
    result = []
    timestamp = query_result[0][1][:10] if len(query_result) else None
    # "__time", "tertiary_class", "period", "probabilistic", "deterministic"
    if timestamp:
        # timestamp_obj = datetime.strptime(timestamp, "%Y-%m-%d")
        for i in query_result:
            result.append({
                "timestamp": timestamp,
                "forecast_period": i[3],
                "prob": i[4],
                "tertiary_class": class_mapping[i[2]],
                "forecast_rain": i[5], 
            })
    sorted_result = sorted(result, key=lambda x: (period_mapping[x["forecast_period"]] - 1) % 12 + 1)
    return sorted_result
    


def gfs_collective(**kwargs):
    referred_date = kwargs.get("referred_date")
    query_result = kwargs.get("query_result")
    result = [{
        "index": f"f{str(n).zfill(2)}",
        "date": i[1],
        "rainfall": i[2],
        "temp_max": i[3],
        "temp_min": i[4],
        "humidity": i[5],
        "latitude": i[6],
        "longitude": i[7],
        "commune": i[8],
        "arrondissement": i[9],
        "department": i[10],
        "region": i[11],
    } for n, i in enumerate(query_result)]
    result = list(filter(lambda x: x.get("date") >= referred_date, result))
    return result


def gfs_rf(**kwargs):
    referred_date = kwargs.get("referred_date")
    query_result = kwargs.get("query_result")
    result = [{
        "index": f"f{str(n).zfill(2)}",
        "date": i[1],
        "rainfall": i[2]
    } for n, i in enumerate(query_result)]
    result = list(filter(lambda x: x.get("date") >= referred_date, result))
    return result


def gfs_temp(**kwargs):
    referred_date = kwargs.get("referred_date")
    query_result = kwargs.get("query_result")
    result = [{
        "index": f"f{str(n).zfill(2)}",
        "date": i[1],
        "temp_max": i[2],
        "temp_min": i[3]
    } for n, i in enumerate(query_result)]
    result = list(filter(lambda x: x.get("date") >= referred_date, result))
    return result


def gfs_rh(**kwargs):
    referred_date = kwargs.get("referred_date")
    query_result = kwargs.get("query_result")
    result = [{
        "index": f"f{str(n).zfill(2)}",
        "date": i[1],
        "humidity": i[2]
    } for n, i in enumerate(query_result)]
    result = list(filter(lambda x: x.get("date") >= referred_date, result))
    return result
