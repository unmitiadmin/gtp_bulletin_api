from rest_framework.views import APIView
from rest_framework.response import Response
from .models import TblUser, TblUserRole
from .serializers import SelfUserSerializer
from django.utils.timezone import now, datetime, timedelta
from django.db import IntegrityError
from rest_framework.exceptions import AuthenticationFailed
import jwt
from jwt.exceptions import ExpiredSignatureError, InvalidTokenError
from .exceptions import NotFilledException, NotFoundException, IncorrectPasswordException
from .utils import is_email_valid, is_password_valid
from gtp_bulletin_api.settings import SECRET_KEY
from pprint import pprint


class RegisterView(APIView):
    def post(self, request):
        name = request.data.get("name")
        email = request.data.get("email")
        password = request.data.get("password")
        phone = request.data.get("phone")
        role_id = request.data.get("role_id")
        try:
            unfilled_fields = []
            if not name: unfilled_fields.append("Name")
            if not email: unfilled_fields.append("Email")
            if not password: unfilled_fields.append("Password")
            if unfilled_fields: raise NotFilledException
            if not is_email_valid(email): 
                return Response({"success": 0, "message": "Please enter a valid email address"}, status=400)
            if not is_password_valid(password):
                return Response({"success": 0, "message": "Please include at least one of each - uppercase letter, lowecase letter, number. Mimimum characters"}, status=400)
            user = TblUser.objects.create(name=name, email=email, phone=phone, role_id=(role_id or 2), created_on=now())
            user.set_password(password)
            user.save()
            return Response({"success": 1, "message": f"User {email} registered successfully"}, status=200)
        except NotFilledException:
            return Response({"success": 0, "message": f"Please fill these details: {', '.join(unfilled_fields)}"}, status=400)
        except IntegrityError:
            return Response({"success": 0, "message": f"User with email {email} is already registered"}, status=400)



class LoginView(APIView):
    def post(self, request):
        email = request.data.get("email")
        password = request.data.get("password")
        try:
            if not email or not password: raise NotFilledException
            user = TblUser.objects.get(email=email)
            if not user: raise NotFoundException
            if not user.status:
                return Response({"success": 0, "message": "Your account is inactive. Please contact admin"}, status=401)
            if not user.check_password(password): raise AuthenticationFailed
            payload = {
                "user_id": user.id,
                "iat": datetime.now(),
                "exp": datetime.now() + timedelta(hours=12),
            }
            token = jwt.encode(payload=payload, key=SECRET_KEY, algorithm="HS256")
            user.previous_login = user.last_login
            user.last_login = datetime.now()
            user.save() 
            return Response({"success": 1, "message": "Logged in successfully", "token": token}, status=200)
        except NotFilledException:
            return Response({"success": 0, "message": "Please enter valid email and password"}, 400)
        except (NotFoundException, TblUser.DoesNotExist):
            return Response({"success": 0, "message": f"User with email {email} does not exist"}, 404)
        except AuthenticationFailed:
            return Response({"success": 0, "message": "Incorrect password, please try again"}, status=400)


class SelfView(APIView):
    def validate_user(self, **kwargs):
        token = kwargs.get("token")
        payload = jwt.decode(token, key=SECRET_KEY, algorithms=["HS256"], options={"verify_exp": True})
        exp_time = datetime.utcfromtimestamp(payload.get("exp"))
        if exp_time < datetime.now(): raise ExpiredSignatureError
        user_id = payload.get("user_id")
        return user_id

    
    def get(self, request):
        try:
            token = request.headers.get("Authorization")
            logged_in_user_id = self.validate_user(token=token)
            user = TblUser.objects.get(pk=logged_in_user_id)
            if not user: raise NotFoundException
            data = SelfUserSerializer(user, many=False).data
            return Response({"success": 1, "data": data}, status=200)
        except (NotFoundException, TblUser.DoesNotExist):
            return Response({"success": 0, "message": "User no longer exists in the database"}, status=404)
        except ExpiredSignatureError:
            return Response({"success": 0, "message": "Your authentication token has expired, please login again"}, status=401)
        except InvalidTokenError:
            return Response({"success": 0, "message": "Your authentication is invalid, please login again"}, status=401)