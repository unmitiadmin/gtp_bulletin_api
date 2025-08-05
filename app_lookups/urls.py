from django.urls import path
from . import views

urlpatterns = [
    # timescale
    path("/months", views.months, name="lkp_months"),
    path("/dekads", views.dekads, name="lkp_dekads"),
    path("/weeks", views.weeks, name="lkp_weeks"),
    # crops
    path("/crops", views.crops, name="lkp_crops"),
    # locations
    path("/regions", views.regions, name="lkp_regions"),
    path("/departments", views.departments, name="lkp_departments"),
    path("/arrondissements", views.arrondissements, name="lkp_arrondissements"),
    path("/communes", views.communes, name="lkp_communes"),
]
