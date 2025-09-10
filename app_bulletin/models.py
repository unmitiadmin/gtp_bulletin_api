from django.db import models
from django.utils.timezone import now


class TblBulletinTemplate(models.Model):
    country = models.ForeignKey('app_lookups.LkpCountry', models.DO_NOTHING)
    template_title = models.TextField(blank=False, null=False)
    template_json = models.JSONField(blank=True, null=True)
    created_on = models.DateTimeField(blank=False, null=False, default=now)
    created_by_user = models.ForeignKey('app_auth.TblUser', models.DO_NOTHING, related_name='fk_template_created_by_user')
    updated_on = models.DateTimeField(blank=True, null=True)
    updated_by_user = models.ForeignKey('app_auth.TblUser', models.DO_NOTHING, related_name='fk_template_updated_by_user', blank=True, null=True)
    deleted_on = models.DateTimeField(blank=True, null=True)
    deleted_by_user = models.ForeignKey('app_auth.TblUser', models.DO_NOTHING, related_name='fk_template_deleted_by_user', blank=True, null=True)
    status = models.BooleanField(blank=True, null=True, default=True)

    class Meta:
        managed = False
        db_table = 'tbl_bulletin_template'


class TblBulletinReport(models.Model):
    country = models.ForeignKey('app_lookups.LkpCountry', models.DO_NOTHING)
    report_title = models.TextField(blank=True, null=True)
    report_json = models.JSONField(blank=True, null=True)
    template = models.ForeignKey('app_bulletin.TblBulletinTemplate', models.DO_NOTHING, blank=True, null=True)
    created_on = models.DateTimeField(blank=False, null=False, default=now)
    created_by_user = models.ForeignKey('app_auth.TblUser', models.DO_NOTHING, )
    updated_on = models.DateTimeField(blank=True, null=True)
    updated_by_user = models.ForeignKey('app_auth.TblUser', models.DO_NOTHING, related_name='tblbulletinreport_updated_by_user_set', blank=True, null=True)
    deleted_on = models.DateTimeField(blank=True, null=True)
    deleted_by_user = models.ForeignKey('app_auth.TblUser', models.DO_NOTHING, related_name='tblbulletinreport_deleted_by_user_set', blank=True, null=True)
    status = models.IntegerField(blank=True, null=True, default=True)

    class Meta:
        managed = False
        db_table = 'tbl_bulletin_report'
