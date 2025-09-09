from django.views.decorators.csrf import csrf_exempt
from rest_framework.decorators import api_view
from rest_framework.response import Response
from app_lookups.constants import valid_admin_levels
from django.utils.timezone import now
from datetime import datetime, timedelta
from dotenv import dotenv_values


env = dict(dotenv_values())
current_year = now().year
first_year = now().year - int(env["YEARS_AGO_FOR_FIRST_YEAR"])

@csrf_exempt
@api_view(['POST'])
def historic_yearly_rainfall(request):
    admin_level = request.data.get("adminLevel")
    admin_level_id = request.data.get("adminLevelId")
    data_src_table = request.data.get("dataSrcId")
    rf_gte = float(request.data.get("rfGte")) or 0
    rf_lt = float(request.data.get("rfLt")) or 0
    from_week = int(request.data.get("fromWeek"))  or 1
    to_week = int(request.data.get("toWeek")) or 52
    from_dekad = int(request.data.get("fromDekad")) or 1
    to_dekad = int(request.data.get("toDekad")) or 36
    from_month = int(request.data.get("fromMonth"))  or 1
    to_month = int(request.data.get("toMonth"))  or 12
    from_date = request.data.get("fromDate") or f"{first_year}-01-01"
    to_date = request.data.get("toDate") or f"{current_year-1}-12-31"
    if admin_level not in valid_admin_levels:
        return Response({"status": 0, "message": f"Please choose a valid admin level - {'/'.join(valid_admin_levels)}"}, status=400)
    
