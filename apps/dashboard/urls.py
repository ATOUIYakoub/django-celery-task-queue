"""
URL configuration for the dashboard app.

  GET  /api/dashboard/dlq/                       → DLQListView
  POST /api/dashboard/dlq/{task_id}/requeue/     → DLQRequeueView
  GET  /api/dashboard/stats/                     → DashboardStatsView
"""

from django.urls import path

from .views import DLQListView, DLQRequeueView, DashboardStatsView

urlpatterns = [
    path("dlq/",                          DLQListView.as_view(),      name="dashboard-dlq-list"),
    path("dlq/<uuid:task_id>/requeue/",   DLQRequeueView.as_view(),   name="dashboard-dlq-requeue"),
    path("stats/",                        DashboardStatsView.as_view(), name="dashboard-stats"),
]

