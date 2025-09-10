from rest_framework.views import APIView
from rest_framework.response import Response
from ..models import TblBulletinReport
from ..serializers import TblReportListSerializer, TblReportDetailSerializer
from ..exceptions import NotFilledException, NotFoundException
from app_auth.models import TblUser
from app_lookups.models import LkpCountry
from gtp_bulletin_api.settings import BASE_DIR, SECRET_KEY
from jwt.exceptions import ExpiredSignatureError, InvalidTokenError
from django.utils.timezone import now, datetime
import jwt
import os
import json
from pprint import pprint


class BulletinReportView(APIView):
    def validate_user(self, **kwargs):
        token = kwargs.get("token")
        payload = jwt.decode(token, key=SECRET_KEY, algorithms=["HS256"], options={"verify_exp": True})
        exp_time = datetime.utcfromtimestamp(payload.get("exp"))
        if exp_time < datetime.now(): raise ExpiredSignatureError
        user_id = payload.get("user_id")
        return user_id
    

    def post(self, request):
        token = request.headers.get("Authorization")
        country_id = request.data.get("country_id")
        template_id = request.data.get("template_id")
        report_title = request.data.get("report_title")
        report_json = request.data.get("report_json")
        try:
            logged_in_user_id = self.validate_user(token=token)
            user = TblUser.objects.get(pk=logged_in_user_id)
            if not user:
                return Response({"success": 0, "message": "You are unauthorized to add this data"}, status=401)
            if not country_id:
                return Response({"success": 0, "message": "Please choose a country"}, status=400)
            country = LkpCountry.objects.get(pk=country_id)
            if not country:
                return Response({"success": 0, "message": "Please select a valid country"}, status=400)
            if not report_title:
                return Response({"success": 0, "message": "Please enter a title for report"}, status=400)
            already_exists = TblBulletinReport.objects.filter(status=1, report_title=report_title).exists()
            if already_exists:
                return Response({"success": 0, "message": "A report with this name already exists, please enter a different title"}, status=409)
            new_report = TblBulletinReport(
                country_id=country_id, template_id=template_id,
                report_title=report_title, report_json=report_json,
                created_on=now(), created_by_user_id=logged_in_user_id
            )
            new_report.save()
            return Response({"success": 1, "message": "Report created"}, status=200)
        except ExpiredSignatureError:
            return Response({"success": 0, "message": "Your authentication token has expired, please login again"}, status=401)
        except InvalidTokenError:
            return Response({"success": 0, "message": "Your authentication token is invalid, please login again"}, status=401)
        
    
    def get(self, request):
        report_id = request.GET.get("report_id")
        try:
            if report_id:
                report_obj = TblBulletinReport.objects.get(pk=report_id, status=1)
                if not report_obj: 
                    raise TblBulletinReport.DoesNotExist
                data = TblReportDetailSerializer(report_obj, many=False).data
            else:
                template_list = TblBulletinReport.objects.filter(status=1)
                data = TblReportListSerializer(template_list, many=True).data
            return Response({"success": 1, "data": data}, status=200)
        except (NotFoundException, TblBulletinReport.DoesNotExist):
            return Response({"success": 0, "message": f"Report with given id does not exist in the database"}, status=404)
        
    
    def patch(self, request):
        token = request.headers.get("Authorization")
        report_id = request.data.get("report_id")
        report_title = request.data.get("report_title")
        report_json = request.data.get("report_json")
        try:
            logged_in_user_id = self.validate_user(token=token)
            user = TblUser.objects.get(pk=logged_in_user_id)
            if not user:
                return Response({"success": 0, "message": "You are unauthorized to add this data"}, status=401)
            report_obj = TblBulletinReport.objects.get(pk=report_id)
            if not report_obj: 
                raise TblBulletinReport.DoesNotExist
            if logged_in_user_id != report_obj.created_by_user.pk:
                return Response({"success": 0, "message": "You cannot edit this report, its created by other user"}, status=401)
            if not report_title:
                return Response({"success": 0, "message": "Please enter a title for report"}, status=400)
            already_exists = TblBulletinReport.objects.filter(status=1, report_title=report_title).exists()
            if already_exists:
                return Response({"success": 0, "message": "A report with this name already exists, please enter a different title"}, status=409)
            report_obj.report_title = report_title
            report_obj.report_json = report_json
            report_obj.updated_by_user_id = logged_in_user_id
            report_obj.updated_on = now()
            report_obj.save()
            return Response({"success": 1, "message": f"Report {report_obj.report_title} has been successfully updated"}, status=200)
        except (NotFoundException, TblBulletinReport.DoesNotExist):
            return Response({"success": 0, "message": f"Report with given id does not exist in the database"}, status=404)
        except ExpiredSignatureError:
            return Response({"success": 0, "message": "Your authentication token has expired, please login again"}, status=401)
        except InvalidTokenError:
            return Response({"success": 0, "message": "Your authentication token is invalid, please login again"}, status=401)
    

    def delete(self, request):
        token = request.headers.get("Authorization")
        report_id = request.data.get("report_id")
        try:
            logged_in_user_id = self.validate_user(token=token)
            user = TblUser.objects.get(pk=logged_in_user_id)
            if not user:
                return Response({"success": 0, "message": "You are unauthorized to add this data"}, status=401)
            report_obj = TblBulletinReport.objects.get(pk=report_id)
            if not report_obj: 
                raise TblBulletinReport.DoesNotExist
            report_obj.status = False
            report_obj.deleted_by_user_id = logged_in_user_id
            report_obj.deleted_on = now()
            report_obj.save()
            return Response({"success": 1, "message": "Report has been successfully deleted"}, status=200)
        except (NotFoundException, TblBulletinReport.DoesNotExist):
            return Response({"success": 0, "message": f"Report with given id does not exist in the database"}, status=404)
        except ExpiredSignatureError:
            return Response({"success": 0, "message": "Your authentication token has expired, please login again"}, status=401)
        except InvalidTokenError:
            return Response({"success": 0, "message": "Your authentication token is invalid, please login again"}, status=401)
    