"""
SpinWatch DRF API Browser — Root URL Configuration
"""
from django.urls import path, include
from rest_framework.routers import DefaultRouter

urlpatterns = [
    path("api/", include("endpoints.urls")),
]
