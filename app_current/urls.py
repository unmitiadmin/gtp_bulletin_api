from django.urls import path
from . import views


urlpatterns = [
    path("current_monthly_rainfall", views.current_monthly_rainfall, name="current_monthly_rainfall"),
    path("current_dekadal_rainfall", views.current_dekadal_rainfall, name="current_dekadal_rainfall"),
    path("current_weekly_rainfall", views.current_weekly_rainfall, name="current_weekly_rainfall"),
    path("current_daily_rainfall", views.current_daily_rainfall, name="current_daily_rainfall"),
]