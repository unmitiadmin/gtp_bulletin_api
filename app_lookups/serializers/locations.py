from rest_framework import serializers
from ..models import LkpRegion, LkpDepartment, LkpArrondissement, LkpCommune


class LkpRegionSerializer(serializers.ModelSerializer):
    class Meta:
        model = LkpRegion
        fields = "__all__"

    def to_representation(self, instance):
        return {
            "region_id": instance.id,
            "region": instance.region
        }


class LkpDepartmentSerializer(serializers.ModelSerializer):
    class Meta:
        model = LkpDepartment
        fields = "__all__"

    def to_representation(self, instance):
        return {
            "department_id": instance.id,
            "department": instance.department,
            "region_id": instance.region_id
        }


class LkpArrondissementSerializer(serializers.ModelSerializer):
    class Meta:
        model = LkpArrondissement
        fields = "__all__"

    def to_representation(self, instance):
        return {
            "arrondissement_id": instance.id,
            "arrondissement": instance.arrondissement,
            "department_id": instance.department_id,
            "region_id": instance.department.region_id if instance.department else None
        }


class LkpCommuneSerializer(serializers.ModelSerializer):
    class Meta:
        model = LkpCommune
        fields = "__all__"

    def to_representation(self, instance):
        return {
            "commune_id": instance.id,
            "commune": instance.commune,
            "arrondissement_id": instance.arrondissement_id,
            "department_id": instance.arrondissement.department_id if instance.arrondissement else None,
            "region_id": instance.arrondissement.department.region_id if instance.arrondissement and instance.arrondissement.department else None
        }
