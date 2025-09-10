from rest_framework import serializers
from .models import TblBulletinTemplate, TblBulletinReport
from django.utils.timezone import datetime

# Bulletins

class TblTemplateDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = TblBulletinTemplate
        fields = "__all__"

    def to_representation(self, instance):
        representation = dict()
        representation["country_id"] = instance.country.pk
        representation["country"] = instance.country.country
        representation["template_title"] = instance.template_title
        representation["template_json"] = instance.template_json
        representation["created_by"] = instance.created_by_user.name
        representation["created_on"] = (
            datetime.strftime(instance.created_on, "%Y-%m-%d %H:%M")
            if instance.created_on else None
        )
        if instance.updated_on:
            representation["last_updated_by"] = instance.updated_by_user.name
            representation["last_updated_on"] = (
                datetime.strftime(instance.updated_on, "%Y-%m-%d %H:%M")
                if instance.updated_on else None
            )
        return representation


class TblTemplateListSerializer(serializers.ModelSerializer):
    class Meta:
        model = TblBulletinTemplate
        fields = "__all__"

    def to_representation(self, instance):
        representation = dict()
        representation["template_id"] = instance.pk
        representation["template_title"] = instance.template_title
        representation["country_id"] = instance.country.pk
        representation["country"] = instance.country.country
        representation["created_by"] = instance.created_by_user.name
        representation["created_on"] = (
            datetime.strftime(instance.created_on, "%Y-%m-%d %H:%M")
            if instance.created_on else None
        )
        if instance.updated_on:
            representation["last_updated_by"] = instance.updated_by_user.name
            representation["last_updated_on"] = (
                datetime.strftime(instance.updated_on, "%Y-%m-%d %H:%M")
                if instance.updated_on else None
            )
        return representation
    

# Reports
    
class TblReportDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = TblBulletinReport
        fields = "__all__"

    def to_representation(self, instance):
        representation = dict()
        representation["country_id"] = instance.country.pk
        representation["country"] = instance.country.country
        representation["report_title"] = instance.report_title
        representation["report_json"] = instance.report_json
        representation["template_id"] = instance.template.pk if instance.template else None
        representation["template_title"] = instance.template.template if instance.template else None
        representation["created_by"] = instance.created_by_user.name
        representation["created_on"] = (
            datetime.strftime(instance.created_on, "%Y-%m-%d %H:%M")
            if instance.created_on else None
        )
        if instance.updated_on:
            representation["last_updated_by"] = instance.updated_by_user.name
            representation["last_updated_on"] = (
                datetime.strftime(instance.updated_on, "%Y-%m-%d %H:%M")
                if instance.updated_on else None
            )
        return representation



class TblReportListSerializer(serializers.ModelSerializer):
    class Meta:
        model = TblBulletinReport
        fields = "__all__"

    def to_representation(self, instance):
        representation = dict()
        representation["report_id"] = instance.pk        
        representation["report_title"] = instance.report_title
        representation["template_id"] = instance.template.pk if instance.template else None
        representation["template_title"] = instance.template.template_title if instance.template else None
        representation["country_id"] = instance.country.pk
        representation["country"] = instance.country.country
        representation["created_by"] = instance.created_by_user.name
        representation["created_on"] = (
            datetime.strftime(instance.created_on, "%Y-%m-%d %H:%M")
            if instance.created_on else None
        )
        if instance.updated_on:
            representation["last_updated_by"] = instance.updated_by_user.name
            representation["last_updated_on"] = (
                datetime.strftime(instance.updated_on, "%Y-%m-%d %H:%M")
                if instance.updated_on else None
            )
        return representation
