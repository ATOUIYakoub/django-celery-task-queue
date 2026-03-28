"""
DRF views for the tasks app.

Endpoints:
  GET    /api/tasks/               → TaskViewSet.list
  GET    /api/tasks/{id}/          → TaskViewSet.retrieve
  POST   /api/tasks/submit/        → TaskSubmitView
  POST   /api/tasks/{id}/cancel/   → TaskCancelView
  POST   /api/tasks/{id}/retry/    → TaskRetryView
  GET    /api/tasks/stats/         → TaskStatsView
"""

import logging

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.db.models import Avg, Q
from django.utils import timezone
from rest_framework import status, viewsets
from rest_framework.exceptions import NotFound, ValidationError
from rest_framework.generics import CreateAPIView
from rest_framework.response import Response
from rest_framework.views import APIView

from .models import TaskRecord
from .serializers import TaskRecordSerializer, TaskSubmitSerializer

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _get_task_or_404(pk) -> TaskRecord:
    try:
        return TaskRecord.objects.get(pk=pk)
    except TaskRecord.DoesNotExist:
        raise NotFound(detail=f"TaskRecord {pk} not found.")


def _dispatch_celery_task(record: TaskRecord):
    """
    Route a TaskRecord to the correct Celery task function based on task_type,
    applying the right queue based on priority.
    """
    from .tasks import (
        export_data_task,
        process_file_task,
        send_email_batch_task,
    )

    PRIORITY_QUEUE_MAP = {
        TaskRecord.Priority.HIGH:    "high",
        TaskRecord.Priority.DEFAULT: "default",
        TaskRecord.Priority.LOW:     "low",
    }
    queue = PRIORITY_QUEUE_MAP.get(record.priority, "default")

    TASK_MAP = {
        TaskRecord.TaskType.FILE_PROCESSING: process_file_task,
        TaskRecord.TaskType.DATA_EXPORT:     export_data_task,
        TaskRecord.TaskType.EMAIL_BATCH:     send_email_batch_task,
    }

    task_fn = TASK_MAP.get(record.task_type)
    if task_fn is None:
        raise ValidationError(f"Unknown task_type '{record.task_type}'.")

    # email_batch always goes to high regardless of priority field
    if record.task_type == TaskRecord.TaskType.EMAIL_BATCH:
        queue = "high"
    elif record.task_type == TaskRecord.TaskType.DATA_EXPORT:
        queue = "low"

    result = task_fn.apply_async(
        args=[str(record.id), record.payload],
        queue=queue,
    )
    return result.id


# ---------------------------------------------------------------------------
# List / Retrieve
# ---------------------------------------------------------------------------
class TaskViewSet(viewsets.ReadOnlyModelViewSet):
    """
    GET /api/tasks/        → list with optional filters
    GET /api/tasks/{id}/   → retrieve single task
    """

    serializer_class = TaskRecordSerializer
    queryset = TaskRecord.objects.all()

    def get_queryset(self):
        qs = TaskRecord.objects.all()
        params = self.request.query_params

        status_filter    = params.get("status")
        task_type_filter = params.get("task_type")
        priority_filter  = params.get("priority")

        if status_filter:
            qs = qs.filter(status=status_filter)
        if task_type_filter:
            qs = qs.filter(task_type=task_type_filter)
        if priority_filter:
            try:
                qs = qs.filter(priority=int(priority_filter))
            except ValueError:
                pass  # ignore bad priority query param

        return qs


# ---------------------------------------------------------------------------
# Submit
# ---------------------------------------------------------------------------
class TaskSubmitView(CreateAPIView):
    """
    POST /api/tasks/submit/
    Create a TaskRecord and dispatch the Celery job.
    Returns 201 with task id, status, and WebSocket URL.
    """

    serializer_class = TaskSubmitSerializer

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data

        # Create the DB record
        record = TaskRecord.objects.create(
            name=data["name"],
            task_type=data["task_type"],
            priority=data["priority"],
            payload=data["payload"],
            status=TaskRecord.Status.PENDING,
        )

        # Dispatch Celery task
        try:
            celery_task_id = _dispatch_celery_task(record)
            record.celery_task_id = celery_task_id
            record.status = TaskRecord.Status.SENT
            record.save(update_fields=["celery_task_id", "status", "updated_at"])
        except Exception as exc:
            logger.error("Failed to dispatch task %s: %s", record.id, exc)
            record.status = TaskRecord.Status.FAILURE
            record.error_message = str(exc)
            record.save(update_fields=["status", "error_message", "updated_at"])
            return Response(
                {"error": str(exc)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )

        ws_host = request.get_host()
        ws_url = f"ws://{ws_host}/ws/tasks/{record.id}/"

        return Response(
            {
                "id": str(record.id),
                "status": record.status,
                "celery_task_id": record.celery_task_id,
                "ws_url": ws_url,
            },
            status=status.HTTP_201_CREATED,
        )


# ---------------------------------------------------------------------------
# Cancel
# ---------------------------------------------------------------------------
class TaskCancelView(APIView):
    """
    POST /api/tasks/{id}/cancel/
    Revoke the Celery task, update status, notify WebSocket clients.
    """

    def post(self, request, pk):
        record = _get_task_or_404(pk)

        if record.celery_task_id:
            from celery.app.control import Control
            from config.celery import app as celery_app

            control = Control(celery_app)
            control.revoke(record.celery_task_id, terminate=True, signal="SIGTERM")

        record.status = TaskRecord.Status.REVOKED
        record.completed_at = timezone.now()
        record.save(update_fields=["status", "completed_at", "updated_at"])

        # Notify any connected WebSocket clients
        channel_layer = get_channel_layer()
        if channel_layer:
            try:
                async_to_sync(channel_layer.group_send)(
                    f"task_{record.id}",
                    {
                        "type": "task.update",
                        "task_id": str(record.id),
                        "status": record.status,
                        "progress": record.progress,
                        "message": "Task cancelled via API.",
                    },
                )
            except Exception as exc:
                logger.warning("Channel notify failed for cancel %s: %s", record.id, exc)

        return Response(
            {"id": str(record.id), "status": record.status},
            status=status.HTTP_200_OK,
        )


# ---------------------------------------------------------------------------
# Retry
# ---------------------------------------------------------------------------
class TaskRetryView(APIView):
    """
    POST /api/tasks/{id}/retry/
    Only allowed when status is 'failure' or 'revoked'.
    Re-creates a fresh TaskRecord with the same payload and dispatches it.
    """

    def post(self, request, pk):
        original = _get_task_or_404(pk)

        if original.status not in (
            TaskRecord.Status.FAILURE,
            TaskRecord.Status.REVOKED,
        ):
            return Response(
                {"error": f"Cannot retry a task with status '{original.status}'."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        new_record = TaskRecord.objects.create(
            name=f"{original.name} (retry)",
            task_type=original.task_type,
            priority=original.priority,
            payload=original.payload,
            status=TaskRecord.Status.PENDING,
        )

        try:
            celery_task_id = _dispatch_celery_task(new_record)
            new_record.celery_task_id = celery_task_id
            new_record.status = TaskRecord.Status.SENT
            new_record.save(update_fields=["celery_task_id", "status", "updated_at"])
        except Exception as exc:
            logger.error("Retry dispatch failed for %s: %s", new_record.id, exc)
            new_record.status = TaskRecord.Status.FAILURE
            new_record.error_message = str(exc)
            new_record.save(update_fields=["status", "error_message", "updated_at"])
            return Response({"error": str(exc)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        ws_host = request.get_host()
        return Response(
            {
                "original_id": str(original.id),
                "new_id": str(new_record.id),
                "status": new_record.status,
                "celery_task_id": new_record.celery_task_id,
                "ws_url": f"ws://{ws_host}/ws/tasks/{new_record.id}/",
            },
            status=status.HTTP_201_CREATED,
        )


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------
class TaskStatsView(APIView):
    """
    GET /api/tasks/stats/
    Returns aggregate counts, average duration, and DLQ depth.
    """

    def get(self, request):
        import redis as redis_module
        from django.conf import settings as django_settings

        qs = TaskRecord.objects.all()
        total = qs.count()

        # Count by status
        by_status = {}
        for choice_value, _ in TaskRecord.Status.choices:
            by_status[choice_value] = qs.filter(status=choice_value).count()

        # Count by task_type
        by_type = {}
        for choice_value, _ in TaskRecord.TaskType.choices:
            by_type[choice_value] = qs.filter(task_type=choice_value).count()

        # Average duration (only for completed tasks)
        completed = qs.filter(
            started_at__isnull=False,
            completed_at__isnull=False,
        )
        avg_duration = None
        if completed.exists():
            durations = [
                r.duration_seconds()
                for r in completed.only("started_at", "completed_at")
                if r.duration_seconds() is not None
            ]
            avg_duration = sum(durations) / len(durations) if durations else None

        # DLQ depth from Redis
        dlq_count = 0
        try:
            r = redis_module.from_url(django_settings.CELERY_BROKER_URL)
            dlq_count = r.llen("dlq:tasks")
        except Exception as exc:
            logger.warning("Could not fetch DLQ depth: %s", exc)

        return Response(
            {
                "total": total,
                "by_status": by_status,
                "by_type": by_type,
                "avg_duration_seconds": avg_duration,
                "dlq_count": dlq_count,
            }
        )
