from rest_framework.decorators import api_view
from rest_framework.response import Response
from ..models import LkpRegion, LkpDepartment, LkpArrondissement, LkpCommune
from ..serializers import LkpRegionSerializer, LkpDepartmentSerializer, LkpArrondissementSerializer, LkpCommuneSerializer


@api_view(["GET"])
def regions(request):
    queryset = LkpRegion.objects.all()
    data = LkpRegionSerializer(queryset, many=True).data
    return Response({"status": 1, "data": data}, status=200)


@api_view(["GET"])
def departments(request):
    region_id = request.GET.get("region_id")
    queryset = LkpDepartment.objects.all()
    if region_id: 
        queryset = queryset.filter(region_id=region_id)
    data = LkpDepartmentSerializer(queryset, many=True).data
    return Response({"status": 1, "data": data})


@api_view(["GET"])
def arrondissements(request):
    region_id = request.GET.get("region_id")
    department_id = request.GET.get("department_id")
    queryset = LkpArrondissement.objects.all()
    if department_id: 
        queryset = queryset.filter(region_id=region_id, department_id=department_id)
    data = LkpArrondissementSerializer(queryset, many=True).data
    return Response({"status": 1, "data": data})



@api_view(["GET"])
def communes(request):
    region_id = request.GET.get("region_id")
    department_id = request.GET.get("department_id")
    arrondissement_id = request.GET.get("arrondissement_id")
    queryset = LkpCommune.objects.all()
    if arrondissement_id:
        queryset = queryset.filter(
            region_id=region_id,
            department_id=department_id,
            arrondissement_id=arrondissement_id
        )
    data = LkpCommuneSerializer(queryset, many=True).data
    return Response({"status": 1, "data": data})
