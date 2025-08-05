from django.db import models


class LkpMonth(models.Model):
    id = models.BigAutoField(primary_key=True)
    max_day = models.IntegerField()
    month_text = models.CharField(max_length=32)

    class Meta:
        managed = False
        db_table = 'lkp_month'


class LkpDekad(models.Model):
    id = models.BigAutoField(primary_key=True)
    dekad_text = models.CharField(max_length=128)
    min_month = models.ForeignKey('LkpMonth', models.DO_NOTHING, db_column='min_month', related_name='lkpdekad_min_month_set')
    max_month = models.ForeignKey('LkpMonth', models.DO_NOTHING, db_column='max_month', related_name='lkpdekad_max_month_set')
    min_day = models.IntegerField()
    max_day = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'lkp_dekad'


class LkpDekadLong(models.Model):
    id = models.BigIntegerField(primary_key=True)
    day = models.IntegerField()
    month = models.ForeignKey('LkpMonth', models.DO_NOTHING, db_column='month')
    met_dekad = models.ForeignKey('LkpDekad', models.DO_NOTHING, db_column='met_dekad')

    class Meta:
        managed = False
        db_table = 'lkp_dekad_long'


class LkpWeek(models.Model):
    id = models.BigAutoField(primary_key=True)
    week_text = models.CharField(max_length=128)
    min_month = models.ForeignKey('LkpMonth', models.DO_NOTHING, db_column='min_month', related_name='lkpweek_min_month_set')
    max_month = models.ForeignKey('LkpMonth', models.DO_NOTHING, db_column='max_month', related_name='lkpweek_max_month_set')
    min_day = models.IntegerField()
    max_day = models.IntegerField()

    class Meta:
        managed = False
        db_table = 'lkp_week'


class LkpWeekLong(models.Model):
    id = models.BigIntegerField(primary_key=True)
    day = models.IntegerField()
    month = models.ForeignKey('LkpMonth', models.DO_NOTHING, db_column='month')
    met_week = models.ForeignKey('LkpWeek', models.DO_NOTHING, db_column='met_week')

    class Meta:
        managed = False
        db_table = 'lkp_week_long'
