from rest_framework import serializers
from ..models import LkpMonth, LkpDekad, LkpWeek


class LkpMonthSerializer(serializers.ModelSerializer):
    class Meta:
        model = LkpMonth
        fields = "__all__"

    def to_representation(self, instance):
        return {
            "month_id": instance.id,
            "month":  instance.month_text,
        }


class LkpDekadSerializer(serializers.ModelSerializer):
    class Meta:
        model = LkpDekad
        fields = "__all__"
    

    def to_representation(self, instance):
        return {
            "dekad_id": instance.id,
            "dekad": instance.dekad_text,
        }


class LkpWeekSerializer(serializers.ModelSerializer):
    class Meta:
        model = LkpWeek
        fields = "__all__"
    
    def to_representation(self, instance):
        return {
            "week_id": instance.id,
            "dekad": instance.week_text
        }
