from rest_framework.decorators import api_view
from rest_framework.response import Response
from ..models import LkpMonth, LkpDekad, LkpWeek
from ..serializers import LkpMonthSerializer, LkpDekadSerializer, LkpWeekSerializer


@api_view(["GET"])
def months(request):
    queryset = LkpMonth.objects.all()
    data = LkpMonthSerializer(queryset, many=True).data
    return Response({"status": 1, "data": data}, status=200)


@api_view(["GET"])
def dekads(request):
    queryset = LkpDekad.objects.all()
    data = LkpDekadSerializer(queryset, many=True).data
    return Response({"status": 1, "data": data}, status=200)


@api_view(["GET"])
def weeks(request):
    queryset = LkpWeek.objects.all()
    data = LkpWeekSerializer(queryset, many=True).data
    return Response({"status": 1, "data": data}, status=200)