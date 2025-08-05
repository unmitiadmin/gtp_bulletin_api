from django.views.decorators.csrf import csrf_exempt
from rest_framework.decorators import api_view
from rest_framework.response import Response
from app_lookups.constants import valid_admin_levels
from .fetch_druid import get_rainfall_forecast, get_temperature_forecast, get_humidity_forecast




@csrf_exempt
@api_view(["POST"])
def forecast_rainfall(request):
    admin_level = request.data.get("adminLevel")
    admin_level_id = request.data.get("adminLevelId")
    data_src_table = request.data.get("dataSrcId")
    if admin_level not in valid_admin_levels:
        return Response({"status": 0, "message": f"Please choose a valid admin level - {'/'.join(valid_admin_levels)}"}, status=400)
    result = get_rainfall_forecast(
        admin_level=admin_level,
        admin_level_id=admin_level_id, 
        data_src_table=data_src_table
    )
    return Response(result, status = 200 if result["status"] else 400)


@csrf_exempt
@api_view(["POST"])
def forecast_temperature(request):
    admin_level = request.data.get("adminLevel")
    admin_level_id = request.data.get("adminLevelId")
    data_src_table = request.data.get("dataSrcId")
    if admin_level not in valid_admin_levels:
        return Response({"status": 0, "message": f"Please choose a valid admin level - {'/'.join(valid_admin_levels)}"}, status=400)
    result = get_temperature_forecast(
        admin_level=admin_level,
        admin_level_id=admin_level_id, 
        data_src_table=data_src_table
    )
    return Response(result, status = 200 if result["status"] else 400)



@csrf_exempt
@api_view(["POST"])
def forecast_humidity(request):
    admin_level = request.data.get("adminLevel")
    admin_level_id = request.data.get("adminLevelId")
    data_src_table = request.data.get("dataSrcId")
    if admin_level not in valid_admin_levels:
        return Response({"status": 0, "message": f"Please choose a valid admin level - {'/'.join(valid_admin_levels)}"}, status=400)
    result = get_humidity_forecast(
        admin_level=admin_level,
        admin_level_id=admin_level_id, 
        data_src_table=data_src_table
    )
    return Response(result, status = 200 if result["status"] else 400)