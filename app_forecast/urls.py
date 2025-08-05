from django.urls import path
from . import views


urlpatterns = [
    path("forecast_rainfall", views.forecast_rainfall, name="forecast_rainfall"),
    path("forecast_temperature", views.forecast_temperature, name="forecast_temperature"),
    path("forecast_humidity", views.forecast_humidity, name="forecast_humidity"),
]