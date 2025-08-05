from rest_framework import serializers
from .models import TblUserRole, TblUser
from django.utils.timezone import datetime


class TblUserSerializer(serializers.ModelSerializer):
    class Meta:
        model = TblUser
        fields = ["id",  "email", "name", "password", "phone"]
        extra_kwargs = {"password": {"write_only": True}}
        validators = [
            serializers.UniqueTogetherValidator(
                queryset=TblUser.objects.all(), 
                fields=["email"], 
                message="User with this email already registered"
            )
        ]

    def create(self, validated_data):
        password = validated_data.pop("password", None)
        instance = self.Meta.model(**validated_data)
        if password is not None:
            instance.set_password(password)
        instance.save()
        return instance


class SelfUserSerializer(serializers.ModelSerializer):
    class Meta:
        model = TblUser

    def to_representation(self, instance):
        representation = dict()
        representation["name"] = instance.name
        representation["email"] = instance.email
        representation["role_id"] = instance.role.pk
        representation["role"] = instance.role.role
        representation["profile_image"] = instance.profile_image or "default_user.png"
        representation["previous_login"] = datetime.strftime(instance.previous_login, "%b %d, %Y %I:%M %p") if instance.previous_login else None
        representation["recent_login"] = datetime.strftime(instance.last_login, "%b %d, %Y %I:%M %p") if instance.last_login else None
        return representation
