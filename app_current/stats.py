from math import sqrt


def get_curr_common_stats(array):
    array = list(filter(lambda x: x is not None, array))
    count = len(array)
    total = round(sum(array), 1)
    min_val = round(min(array), 1) if count else "N/A"
    max_val = round(max(array), 1) if count else "N/A"
    mean_val = round(sum(array)/count, 1) if count else "N/A"
    std_dev_val = round(sqrt(sum([((x - mean_val) ** 2) for x in array]) / count), 1) if count and isinstance(mean_val, (int, float)) and mean_val else "N/A"
    cov_val = round((std_dev_val * 100) / mean_val, 1) if count and isinstance(mean_val, (int, float)) and mean_val else "N/A"
    return {"count": count, "total": total, "min": min_val,  "max": max_val, "mean": mean_val, "std_dev": std_dev_val, "cov": cov_val}


def is_non_leap_year(year):
    return bool(year % 4)
