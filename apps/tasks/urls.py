"""
URL configuration for the tasks app.

Router registers:
  GET  /api/tasks/         → TaskViewSet.list
  GET  /api/tasks/{id}/    → TaskViewSet.retrieve

Explicit paths (must come before router.urls to avoid UUID shadowing):
  POST /api/tasks/submit/           → TaskSubmitView
  POST /api/tasks/stats/            → TaskStatsView   (GET)
  POST /api/tasks/{id}/cancel/      → TaskCancelView
  POST /api/tasks/{id}/retry/       → TaskRetryView
"""

from django.urls import include, path
from rest_framework.routers import DefaultRouter

from .views import TaskCancelView, TaskRetryView, TaskStatsView, TaskSubmitView, TaskViewSet

router = DefaultRouter()
router.register("", TaskViewSet, basename="task")

urlpatterns = [
    # Explicit paths first so they aren't swallowed by the router's {pk} pattern
    path("submit/",                TaskSubmitView.as_view(),  name="task-submit"),
    path("stats/",                 TaskStatsView.as_view(),   name="task-stats"),
    path("<uuid:pk>/cancel/",      TaskCancelView.as_view(),  name="task-cancel"),
    path("<uuid:pk>/retry/",       TaskRetryView.as_view(),   name="task-retry"),
] + router.urls

