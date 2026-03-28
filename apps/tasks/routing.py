"""
Channels WebSocket URL routing for the tasks app.
"""

from django.urls import re_path

from .consumers import TaskProgressConsumer

websocket_urlpatterns = [
    re_path(
        r"ws/tasks/(?P<task_id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/$",
        TaskProgressConsumer.as_asgi(),
    ),
]
