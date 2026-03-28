"""
Root URL configuration.
"""

from django.contrib import admin
from django.urls import include, path

urlpatterns = [
    path("admin/", admin.site.urls),
    path("api/tasks/", include("apps.tasks.urls")),
    path("api/dashboard/", include("apps.dashboard.urls")),
]
