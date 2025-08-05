from rest_framework.decorators import api_view
from rest_framework.response import Response
from ..models import crop_list


@api_view(["GET"])
def crops(request):
    return Response({"status": 1, "data": crop_list}, status=200)
