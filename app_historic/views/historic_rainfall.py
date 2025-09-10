from django.views.decorators.csrf import csrf_exempt
from rest_framework.decorators import api_view
from rest_framework.response import Response
from app_lookups.constants import valid_admin_levels
from django.utils.timezone import now
from datetime import datetime, timedelta
from dotenv import dotenv_values
from ..fetch_druid import (
    get_historic_yearly_rainfall, get_historic_yearly_rainfall_rm, 
    get_historic_yearly_rainfall_rd, get_historic_yearly_rainfall_rw,
    get_historic_dry_spells, get_historic_dry_spells_rd, get_historic_dry_spells_rw,
    get_historic_wet_spells, get_historic_wet_spells_rd, get_historic_wet_spells_rw,
    get_historic_crop_stress
)


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
    if not admin_level or not admin_level_id:
        return Response({"status": 0, "message": "Please choose a location"})
    if admin_level not in valid_admin_levels:
        return Response({"status": 0, "message": f"Please choose a valid admin level - {'/'.join(valid_admin_levels)}"}, status=400)
    if(isinstance(rf_gte, (int, float)) and isinstance(rf_lt, (int, float))):
        if(rf_gte > rf_lt): 
            return Response({"status": 0, "message": "Rainfall limits are invalid, please check"}, status=400)
    if(isinstance(rf_gte, (int, float)) or isinstance(rf_lt, (int, float))):
        if(rf_gte < 0 or rf_lt < 0): 
            return Response({"status": 0, "message": "Rainfall can't be below 0, please enter more than 0"}, status=400)
    if from_month > to_month:
        result = get_historic_yearly_rainfall_rm(
            admin_level=admin_level, admin_level_id=admin_level_id, 
            rf_gte=rf_gte, rf_lt=rf_lt, 
            from_month=from_month, to_month=to_month,
            data_src_table=data_src_table
        )
        return Response(result, status = 200 if result["status"] else 400)
    if from_dekad > to_dekad:
        result = get_historic_yearly_rainfall_rd(
            admin_level=admin_level, admin_level_id=admin_level_id, 
            rf_gte=rf_gte, rf_lt=rf_lt, 
            from_dekad=from_dekad, to_dekad=to_dekad,
            data_src_table=data_src_table
        )
        return Response(result, status = 200 if result["status"] else 400)
    if from_week > to_week:
        result = get_historic_yearly_rainfall_rw(
            admin_level=admin_level, admin_level_id=admin_level_id, 
            rf_gte=rf_gte, rf_lt=rf_lt, 
            from_week=from_week, to_week=to_week, 
            data_src_table=data_src_table
        )
        return Response(result, status = 200 if result["status"] else 400)
    result = get_historic_yearly_rainfall(
        admin_level=admin_level, admin_level_id=admin_level_id,
        rf_gte=rf_gte, rf_lt=rf_lt,
        from_week=from_week, to_week=to_week, 
        from_month=from_month, to_month=to_month,
        from_date=from_date, to_date=to_date,
        from_dekad=from_dekad, to_dekad=to_dekad,
        data_src_table=data_src_table
    )
    return Response(result, status = 200 if result["status"] else 400)

    
    
@csrf_exempt
@api_view(['POST'])
def historic_dry_spells(request):
    admin_level = request.data.get("adminLevel")
    admin_level_id = request.data.get("adminLevelId")
    data_src_table = request.data.get("dataSrcId")
    resolution = request.data.get("resolution") or "week"
    rf_lt = float(request.data.get("rfLt") or 0) or None
    from_week = int(request.data.get("fromWeek"))  or 1
    to_week = int(request.data.get("toWeek")) or 52
    from_dekad = int(request.data.get("fromDekad")) or 1
    to_dekad = int(request.data.get("toDekad")) or 36
    data_src_table = request.data.get("dataSrcId")
    if not admin_level or not admin_level_id:
        return Response({"status": 0, "message": "Please choose a location"})
    if admin_level not in valid_admin_levels:
        return Response({"status": 0, "message": f"Please choose a valid admin level - {'/'.join(valid_admin_levels)}"}, status=400)
    if rf_lt is None: 
        return Response({"status": 0, "message": "Enter Weekly rainfall (mm) less than"}, status=400)
    if(isinstance(rf_lt, (int, float)) and rf_lt < 0): 
        return Response({"status": 0, "message": "Rainfall can't be below 0, please enter more than 0"}, status=400)
    if (from_week > to_week) or (from_dekad > to_dekad):
        if from_week > to_week:
            result = get_historic_dry_spells_rw(
                admin_level=admin_level, admin_level_id=admin_level_id, 
                rf_lt=rf_lt, 
                from_week=from_week, to_week=to_week, 
                data_src_table=data_src_table
            )
        if from_dekad > to_dekad:
            result  = get_historic_dry_spells_rd(
                admin_level=admin_level, admin_level_id=admin_level_id, 
                rf_lt=rf_lt, 
                from_dekad=from_dekad, to_dekad=to_dekad,    
                data_src_table=data_src_table
            )
    else:
        result = get_historic_dry_spells(
            admin_level=admin_level, admin_level_id=admin_level_id, 
            rf_lt=rf_lt, 
            from_week=from_week, to_week=to_week,
            from_dekad=from_dekad, to_dekad=to_dekad,
            resolution=resolution,
            data_src_table=data_src_table
        )
    return Response(result, status = 200 if result["status"] else 400)


@csrf_exempt
@api_view(['POST'])
def historic_wet_spells(request):
    admin_level = request.data.get("adminLevel")
    admin_level_id = request.data.get("adminLevelId")
    data_src_table = request.data.get("dataSrcId")
    resolution = request.data.get("resolution") or "week"
    rf_gte = float(request.data.get("rfGte") or 0) or None
    from_week = int(request.data.get("fromWeek"))  or 1
    to_week = int(request.data.get("toWeek")) or 52
    from_dekad = int(request.data.get("fromDekad")) or 1
    to_dekad = int(request.data.get("toDekad")) or 36
    data_src_table = request.data.get("dataSrcId")
    if not admin_level or not admin_level_id:
        return Response({"status": 0, "message": "Please choose a location"})
    if admin_level not in valid_admin_levels:
        return Response({"status": 0, "message": f"Please choose a valid admin level - {'/'.join(valid_admin_levels)}"}, status=400)
    if rf_gte is None: 
        return Response({"status": 0, "message": "Enter Weekly rainfall (mm) less than"}, status=400)
    if(isinstance(rf_gte, (int, float)) and rf_gte < 0): 
        return Response({"status": 0, "message": "Rainfall can't be below 0, please enter more than 0"}, status=400)
    if (from_week > to_week) or (from_dekad > to_dekad):
        if from_week > to_week:
            result = get_historic_wet_spells_rw(
                admin_level=admin_level, admin_level_id=admin_level_id, 
                rf_gte=rf_gte, 
                from_week=from_week, to_week=to_week, 
                data_src_table=data_src_table
            )
        if from_dekad > to_dekad:
            result  = get_historic_wet_spells_rd(
                admin_level=admin_level, admin_level_id=admin_level_id, 
                rf_gte=rf_gte, 
                from_dekad=from_dekad, to_dekad=to_dekad,    
                data_src_table=data_src_table
            )
    else:
        result = get_historic_wet_spells(
            admin_level=admin_level, admin_level_id=admin_level_id, 
            rf_gte=rf_gte, 
            from_week=from_week, to_week=to_week,
            from_dekad=from_dekad, to_dekad=to_dekad,
            resolution=resolution,
            data_src_table=data_src_table
        )
    return Response(result, status = 200 if result["status"] else 400)


@csrf_exempt
@api_view(['POST'])
def historic_crop_stress(request):
    admin_level = request.data.get("adminLevel")
    admin_level_id = request.data.get("adminLevelId")
    data_src_table = request.data.get("dataSrcId")
    crop_id = int(request.data.get("cropId")) or 0
    rf_lt = float(request.data.get("rfLt")) or 0
    from_week = int(request.data.get("fromWeek"))  or 0
    data_src_table = request.data.get("dataSrcId")
    if not admin_level or not admin_level_id:
        return Response({"status": 0, "message": "Please choose a location"})
    if admin_level not in valid_admin_levels:
        return Response({"status": 0, "message": f"Please choose a valid admin level - {'/'.join(valid_admin_levels)}"}, status=400)
    if not crop_id: 
        return Response({"status": 0, "message": "Select a crop to view data"}, status=400)
    if rf_lt is None: 
        return Response({"status": 0, "message": "Enter Weekly rainfall (mm) less than"}, status=400)
    if not from_week: 
        return Response({"status": 0, "message": "Enter the week to start sowing"}, status=400)
    result = get_historic_crop_stress(
        admin_level=admin_level, admin_level_id=admin_level_id, 
        crop_id=crop_id, from_week=from_week, 
        rf_lt=rf_lt, data_src_table=data_src_table
    )
    return Response(result, status=200) if result["status"] else Response(result, status=500)
