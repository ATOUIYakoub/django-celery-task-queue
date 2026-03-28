"""
Dashboard API views.

Endpoints:
  GET    /api/dashboard/dlq/                        → list all tasks in the Redis DLQ
  POST   /api/dashboard/dlq/{task_id}/requeue/      → move task back to normal queue
  GET    /api/dashboard/stats/                      → Redis + Celery + queue depth stats
"""

import json
import logging

import redis as redis_module
from django.conf import settings
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger(__name__)


def _get_redis():
    return redis_module.from_url(settings.CELERY_BROKER_URL)


# ---------------------------------------------------------------------------
# DLQ
# ---------------------------------------------------------------------------
class DLQListView(APIView):
    """
    GET  /api/dashboard/dlq/
    Returns all tasks currently sitting in the Redis dead-letter queue.

    POST /api/dashboard/dlq/{task_id}/requeue/
    Re-submits a specific task from the DLQ back to its original queue.
    """

    def get(self, request):
        try:
            r = _get_redis()
            raw_entries = r.lrange("dlq:tasks", 0, -1)
            entries = [json.loads(raw) for raw in raw_entries]
        except Exception as exc:
            logger.error("Failed to read DLQ: %s", exc)
            return Response(
                {"error": f"Could not read DLQ: {exc}"},
                status=500,
            )

        return Response({"count": len(entries), "tasks": entries})


class DLQRequeueView(APIView):
    """
    POST /api/dashboard/dlq/{task_id}/requeue/
    Moves a task from the DLQ back to its normal processing queue.
    """

    def post(self, request, task_id):
        from apps.tasks.tasks import requeue_from_dlq

        success = requeue_from_dlq(str(task_id))
        if success:
            return Response(
                {"requeued": True, "task_id": str(task_id)},
            )
        return Response(
            {"requeued": False, "error": f"No DLQ entry found for task_id {task_id}."},
            status=404,
        )


# ---------------------------------------------------------------------------
# Dashboard Stats
# ---------------------------------------------------------------------------
class DashboardStatsView(APIView):
    """
    GET /api/dashboard/stats/
    Aggregates Redis server info, Celery worker state, and per-queue depths.
    """

    def get(self, request):
        # --- Redis info ---
        redis_info = {}
        try:
            r = _get_redis()
            info = r.info()
            redis_info = {
                "connected_clients": info.get("connected_clients"),
                "used_memory_human": info.get("used_memory_human"),
                "redis_version": info.get("redis_version"),
                "uptime_in_seconds": info.get("uptime_in_seconds"),
            }
        except Exception as exc:
            logger.warning("Redis info unavailable: %s", exc)
            redis_info = {"error": str(exc)}

        # --- Celery worker count (via inspect) ---
        worker_count = 0
        worker_names = []
        try:
            from config.celery import app as celery_app
            inspector = celery_app.control.inspect(timeout=2.0)
            active = inspector.active()
            if active:
                worker_count = len(active)
                worker_names = list(active.keys())
        except Exception as exc:
            logger.warning("Celery inspect unavailable: %s", exc)

        # --- Queue depths (approximate via Redis list length) ---
        queue_depths = {}
        queue_names = ["high", "default", "low", "dlq"]
        try:
            r = _get_redis()
            for q in queue_names:
                # Celery with Redis broker stores tasks in a Redis list named after the queue
                queue_depths[q] = r.llen(q)
            # DLQ tasks are in our custom dlq:tasks list
            queue_depths["dlq"] = r.llen("dlq:tasks")
        except Exception as exc:
            logger.warning("Could not read queue depths: %s", exc)
            queue_depths = {"error": str(exc)}

        return Response(
            {
                "redis": redis_info,
                "celery": {
                    "worker_count": worker_count,
                    "workers": worker_names,
                },
                "queue_depths": queue_depths,
            }
        )
