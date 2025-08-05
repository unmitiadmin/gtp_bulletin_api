from django.db import models


class LkpRegion(models.Model):
    id = models.BigAutoField(primary_key=True)
    region = models.CharField(max_length=128)

    class Meta:
        managed = False
        db_table = 'lkp_region'


class LkpDepartment(models.Model):
    id = models.BigAutoField(primary_key=True)
    department = models.CharField(max_length=128)
    region = models.ForeignKey('LkpRegion', models.DO_NOTHING, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'lkp_department'


class LkpArrondissement(models.Model):
    id = models.BigAutoField(primary_key=True)
    arrondissement = models.CharField(max_length=128)
    department = models.ForeignKey('LkpDepartment', models.DO_NOTHING, blank=True, null=True)
    region = models.ForeignKey('LkpRegion', models.DO_NOTHING, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'lkp_arrondissement'


class LkpCommune(models.Model):
    id = models.BigAutoField(primary_key=True)
    commune = models.CharField(max_length=128)
    arrondissement = models.ForeignKey('LkpArrondissement', models.DO_NOTHING, blank=True, null=True)
    department = models.ForeignKey('LkpDepartment', models.DO_NOTHING, blank=True, null=True)
    region = models.ForeignKey('LkpRegion', models.DO_NOTHING, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'lkp_commune'
