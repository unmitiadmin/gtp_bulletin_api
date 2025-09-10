from django.urls import path
from . import views


urlpatterns = [
    path("data/bulletin_template", views.BulletinTemplateView.as_view(), name="data_bulletin_template"),
    path("data/bulletin_report", views.BulletinReportView.as_view(), name="data_bulletin_report"),
]
