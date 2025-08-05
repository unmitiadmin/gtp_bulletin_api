from django.db import models
from django.utils.timezone import now
from django.contrib.auth.models import AbstractBaseUser, BaseUserManager


class TblUserRole(models.Model):
    role = models.CharField(unique=True, max_length=128)
    created_on = models.DateTimeField(default=now)
    updated_on = models.DateTimeField(blank=True, null=True)
    status = models.BooleanField(default=True)

    class Meta:
        managed = False
        db_table = 'tbl_user_role'


class TblUserManager(BaseUserManager):
    def create_user(self, email, name, password, **kwargs):
        if not email: raise ValueError("User must have an email address")
        if not password: raise ValueError("User must have password")
        if not name: raise ValueError("User must have a name")
        user = self.model(email=self.normalize_email(email))
        user.set_password(password)
        user.name = name
        user.phone = kwargs.get("phone")
        user.save(using=self._db)
        return user
    

class TblUser(AbstractBaseUser):
    previous_login = models.DateTimeField(blank=True, null=True)
    last_login = models.DateTimeField(blank=True, null=True)
    name = models.CharField(max_length=128)
    email = models.CharField(unique=True, max_length=128)
    password = models.CharField(max_length=1024)
    phone = models.CharField(max_length=16, blank=True, null=True)
    role = models.ForeignKey('app_auth.TblUserRole', models.DO_NOTHING, blank=True, null=True)
    profile_image = models.CharField(max_length=256, blank=True, null=True)
    created_on = models.DateTimeField(default=now)
    updated_on = models.DateTimeField(blank=True, null=True)
    status = models.BooleanField(default=True)

    USERNAME_FIELD = "email"
    REQUIRED_FIELDS = ["password", "name"]
    objects = TblUserManager()

    class Meta:
        managed = False
        db_table = 'tbl_user'
