from rest_framework.views import APIView
from rest_framework.response import Response
from ..models import TblBulletinTemplate
from ..serializers import TblTemplateDetailSerializer, TblTemplateListSerializer
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


class BulletinTemplateView(APIView):
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
        template_title = request.data.get("template_title")
        template_json = request.data.get("template_json")
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
            if not template_title:
                return Response({"success": 0, "message": "Please enter a title for template"}, status=400)
            already_exists = TblBulletinTemplate.objects.filter(status=1, template_title=template_title).exists()
            if already_exists:
                return Response({"success": 0, "message": "A template with this name already exists, please enter a different title"}, status=409)
            new_template = TblBulletinTemplate(
                country_id=country_id, template_title=template_title, template_json = template_json,
                created_by_user_id=logged_in_user_id, created_on=now()
            )
            new_template.save()
            return Response({"success": 1, "message": "Template created"}, status=200)
        except ExpiredSignatureError:
            return Response({"success": 0, "message": "Your authentication token has expired, please login again"}, status=401)
        except InvalidTokenError:
            return Response({"success": 0, "message": "Your authentication token is invalid, please login again"}, status=401)


    def get(self, request):
        template_id = request.GET.get("template_id")
        try:
            if template_id:
                template_obj = TblBulletinTemplate.objects.get(pk=template_id, status=1)
                if not template_obj: 
                    raise TblBulletinTemplate.DoesNotExist
                data = TblTemplateDetailSerializer(template_obj, many=False).data
            else:
                template_list = TblBulletinTemplate.objects.filter(status=1)
                data = TblTemplateListSerializer(template_list, many=True).data
            return Response({"success": 1, "data": data}, status=200)
        except (NotFoundException, TblBulletinTemplate.DoesNotExist):
            return Response({"success": 0, "message": f"Template with given id does not exist in the database"}, status=404)


    def patch(self, request):
        token = request.headers.get("Authorization")
        template_id = request.data.get("template_id")
        template_title = request.data.get("template_title")
        template_json = request.data.get("template_json")
        try:
            logged_in_user_id = self.validate_user(token=token)
            user = TblUser.objects.get(pk=logged_in_user_id)
            if not user:
                return Response({"success": 0, "message": "You are unauthorized to add this data"}, status=401)
            if not template_id:
                return Response({"success": 0, "message": "Please choose a template to edit"}, status=400)
            template_obj = TblBulletinTemplate.objects.get(pk=template_id)
            if not template_obj: 
                raise TblBulletinTemplate.DoesNotExist
            if logged_in_user_id != template_obj.created_by_user.pk:
                return Response({"success": 0, "message": "You cannot edit this template, its created by other user"}, status=401)
            if not template_title:
                return Response({"success": 0, "message": "Please enter a title for template"}, status=400)
            already_exists = TblBulletinTemplate.objects.filter(status=1, template_title=template_title).exists()
            if already_exists:
                return Response({"success": 0, "message": "A template with this name already exists, please enter a different title"}, status=409)
            template_obj.template_title = template_title
            template_obj.template_json = template_json
            template_obj.updated_by_user_id = logged_in_user_id
            template_obj.updated_on = now()
            template_obj.save()
            return Response({"success": 1, "message": f"Template {template_obj.template_title} has been successfully updated"}, status=200)
        except (NotFoundException, TblBulletinTemplate.DoesNotExist):
            return Response({"success": 0, "message": f"Template with given id does not exist in the database"}, status=404)
        except ExpiredSignatureError:
            return Response({"success": 0, "message": "Your authentication token has expired, please login again"}, status=401)
        except InvalidTokenError:
            return Response({"success": 0, "message": "Your authentication token is invalid, please login again"}, status=401)


    def delete(self, request):
        token = request.headers.get("Authorization")
        template_id = request.data.get("template_id")
        try:
            logged_in_user_id = self.validate_user(token=token)
            user = TblUser.objects.get(pk=logged_in_user_id)
            if not user:
                return Response({"success": 0, "message": "You are unauthorized to add this data"}, status=401)
            if not template_id:
                return Response({"success": 0, "message": "Please choose a template to edit"}, status=400)
            template_obj = TblBulletinTemplate.objects.get(pk=template_id)
            if not template_obj: 
                raise TblBulletinTemplate.DoesNotExist
            if logged_in_user_id != template_obj.created_by_user.pk:
                return Response({"success": 0, "message": "You cannot delete this template, its created by other user"}, status=401)
            template_obj.deleted_by_user_id = logged_in_user_id
            template_obj.deleted_on = now()
            template_obj.status = 0
            template_obj.save()
            return Response({"success": 1, "message": "Template has been successfully deleted"}, status=200)
        except ExpiredSignatureError:
            return Response({"success": 0, "message": "Your authentication token has expired, please login again"}, status=401)
        except InvalidTokenError:
            return Response({"success": 0, "message": "Your authentication token is invalid, please login again"}, status=401)
