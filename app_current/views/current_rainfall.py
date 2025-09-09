from django.views.decorators.csrf import csrf_exempt
from rest_framework.decorators import api_view
from rest_framework.response import Response
from app_lookups.constants import valid_admin_levels
from django.utils.timezone import now
from datetime import datetime, timedelta
from ..fetch_druid import (
    get_current_monthly_rainfall, get_current_dekadal_rainfall, 
    get_current_weekly_rainfall, get_current_daily_rainfall,
)


@csrf_exempt
@api_view(['POST'])
def current_monthly_rainfall(request):
    admin_level = request.data.get("adminLevel")
    admin_level_id = request.data.get("adminLevelId")
    data_src_table = request.data.get("dataSrcId")
    from_month = int(request.data.get("fromMonth"))  or 1
    to_month = int(request.data.get("toMonth"))  or 12
    if not admin_level or not admin_level_id:
        return Response({"status": 0, "message": "Please choose a location"})
    if admin_level not in valid_admin_levels:
        return Response({"status": 0, "message": f"Please choose a valid admin level - {'/'.join(valid_admin_levels)}"}, status=400)
    if from_month > to_month:
        return Response({"status": 0, "message": "From month can't be later than To month"}, status=400)
    result = get_current_monthly_rainfall(
        admin_level=admin_level,
        admin_level_id=admin_level_id,
        from_month=from_month,
        to_month=to_month,
        data_src_table=data_src_table
    )
    return Response(result, status = 200 if result["status"] else 400)
    


@csrf_exempt
@api_view(['POST'])
def current_dekadal_rainfall(request):
    admin_level = request.data.get("adminLevel")
    admin_level_id = request.data.get("adminLevelId")
    data_src_table = request.data.get("dataSrcId")
    from_dekad = int(request.data.get("fromDekad"))  or 1
    to_dekad = int(request.data.get("toDekad"))  or 12
    if not admin_level or not admin_level_id:
        return Response({"status": 0, "message": "Please choose a location"})
    if admin_level not in valid_admin_levels:
        return Response({"status": 0, "message": f"Please choose a valid admin level - {'/'.join(valid_admin_levels)}"}, status=400)
    if from_dekad > to_dekad:
        return Response({"status": 0, "message": "From dekad can't be later than To dekad"}, status=400)
    result = get_current_dekadal_rainfall(
        admin_level=admin_level,
        admin_level_id=admin_level_id,
        from_dekad=from_dekad,
        to_dekad=to_dekad,
        data_src_table=data_src_table
    )
    return Response(result, status = 200 if result["status"] else 400)


@csrf_exempt
@api_view(['POST'])
def current_weekly_rainfall(request):
    admin_level = request.data.get("adminLevel")
    admin_level_id = request.data.get("adminLevelId")
    data_src_table = request.data.get("dataSrcId")
    from_week = int(request.data.get("fromWeek"))  or 1
    to_week = int(request.data.get("toWeek"))  or 12
    if not admin_level or not admin_level_id:
        return Response({"status": 0, "message": "Please choose a location"})
    if admin_level not in valid_admin_levels:
        return Response({"status": 0, "message": f"Please choose a valid admin level - {'/'.join(valid_admin_levels)}"}, status=400)
    if from_week > to_week:
        return Response({"status": 0, "message": "From week can't be later than To week"}, status=400)
    result = get_current_weekly_rainfall(
        admin_level=admin_level,
        admin_level_id=admin_level_id,
        from_week=from_week,
        to_week=to_week,
        data_src_table=data_src_table
    )
    return Response(result, status = 200 if result["status"] else 400)


@csrf_exempt
@api_view(['POST'])
def current_daily_rainfall(request):
    current_year = now().year
    year_start_date = f"{current_year}-01-01"
    prev_day = (now() - timedelta(days=1)).strftime("%Y-%m-%d")
    admin_level = request.data.get("adminLevel")
    admin_level_id = request.data.get("adminLevelId")
    data_src_table = request.data.get("dataSrcId")
    from_date = request.data.get("fromDate") or year_start_date
    to_date = request.data.get("toDate") or prev_day
    if not admin_level or not admin_level_id:
        return Response({"status": 0, "message": "Please choose a location"})
    if admin_level not in valid_admin_levels:
        return Response({"status": 0, "message": f"Please choose a valid admin level - {'/'.join(valid_admin_levels)}"}, status=400)
    if datetime.strptime(from_date, "%Y-%m-%d") > datetime.strptime(to_date, "%Y-%m-%d"):
        return Response({"status": 0, "message": "From date can't be later than To date"}, status=400)
    result = get_current_daily_rainfall(
        admin_level=admin_level,
        admin_level_id=admin_level_id,
        from_date=from_date,
        to_date=to_date,
        data_src_table=data_src_table
    )
    return Response(result, status = 200 if result["status"] else 400)
