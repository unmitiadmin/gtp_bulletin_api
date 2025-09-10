from django.urls import path
from . import views


urlpatterns = [
    path("historic_yearly_rainfall", views.historic_yearly_rainfall, name="historic_yearly_rainfall"),
    path("historic_dry_spells", views.historic_dry_spells, name="historic_dry_spells"),
    path("historic_wet_spells", views.historic_wet_spells, name="historic_wet_spells"),
    path("historic_crop_stress", views.historic_crop_stress, name="historic_crop_stress"),
]