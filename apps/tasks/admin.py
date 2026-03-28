"""
Django admin configuration for the TaskRecord model.

Features:
  - Compact list_display with truncated UUID
  - Filters on status, task_type, priority
  - Full-text search on name and celery_task_id
  - All fields read-only except 'name'
  - Custom bulk actions: retry and revoke
"""

import logging

from django.contrib import admin, messages
from django.utils import timezone

from .models import TaskRecord

logger = logging.getLogger(__name__)


def _short_id(obj) -> str:
    """Return the first 8 characters of the UUID for compact display."""
    return str(obj.id)[:8]


_short_id.short_description = "ID"  # type: ignore[attr-defined]


@admin.register(TaskRecord)
class TaskRecordAdmin(admin.ModelAdmin):

    # -----------------------------------------------------------------------
    # List view
    # -----------------------------------------------------------------------
    list_display = (
        _short_id,
        "name",
        "task_type",
        "status",
        "priority",
        "progress",
        "retry_count",
        "created_at",
        "duration_seconds_display",
    )
    list_filter  = ("status", "task_type", "priority")
    search_fields = ("name", "celery_task_id")
    ordering = ("-created_at",)
    date_hierarchy = "created_at"

    # -----------------------------------------------------------------------
    # Detail view
    # -----------------------------------------------------------------------
    readonly_fields = (
        "id",
        "task_type",
        "priority",
        "status",
        "celery_task_id",
        "progress",
        "progress_message",
        "payload",
        "result",
        "error_message",
        "retry_count",
        "max_retries",
        "created_at",
        "updated_at",
        "started_at",
        "completed_at",
        "duration_seconds_display",
    )

    fieldsets = (
        ("Identity", {
            "fields": ("id", "name", "task_type", "priority"),
        }),
        ("State", {
            "fields": ("status", "celery_task_id", "progress", "progress_message"),
        }),
        ("Data", {
            "fields": ("payload", "result", "error_message"),
        }),
        ("Retry", {
            "fields": ("retry_count", "max_retries"),
        }),
        ("Timestamps", {
            "fields": ("created_at", "updated_at", "started_at", "completed_at", "duration_seconds_display"),
        }),
    )

    # -----------------------------------------------------------------------
    # Custom computed column
    # -----------------------------------------------------------------------
    @admin.display(description="Duration (s)", ordering="completed_at")
    def duration_seconds_display(self, obj) -> str:
        val = obj.duration_seconds()
        return f"{val:.2f}s" if val is not None else "—"

    # -----------------------------------------------------------------------
    # Bulk actions
    # -----------------------------------------------------------------------
    actions = ["retry_tasks", "revoke_tasks"]

    @admin.action(description="Retry selected tasks")
    def retry_tasks(self, request, queryset):
        """Re-submit each selected failed/revoked task with its original payload."""
        from .views import _dispatch_celery_task

        retried = 0
        skipped = 0
        for record in queryset:
            if record.status not in (TaskRecord.Status.FAILURE, TaskRecord.Status.REVOKED):
                skipped += 1
                continue
            new_record = TaskRecord.objects.create(
                name=f"{record.name} (admin retry)",
                task_type=record.task_type,
                priority=record.priority,
                payload=record.payload,
                status=TaskRecord.Status.PENDING,
            )
            try:
                celery_task_id = _dispatch_celery_task(new_record)
                new_record.celery_task_id = celery_task_id
                new_record.status = TaskRecord.Status.SENT
                new_record.save(update_fields=["celery_task_id", "status", "updated_at"])
                retried += 1
            except Exception as exc:
                logger.error("Admin retry failed for %s: %s", record.id, exc)
                new_record.delete()
                skipped += 1

        if retried:
            self.message_user(request, f"{retried} task(s) re-submitted.", messages.SUCCESS)
        if skipped:
            self.message_user(
                request,
                f"{skipped} task(s) skipped (not in failure/revoked status or dispatch error).",
                messages.WARNING,
            )

    @admin.action(description="Revoke selected tasks")
    def revoke_tasks(self, request, queryset):
        """Send SIGTERM to each selected task's Celery worker and mark as revoked."""
        from celery.app.control import Control
        from config.celery import app as celery_app

        control = Control(celery_app)
        revoked = 0
        for record in queryset:
            if record.celery_task_id:
                try:
                    control.revoke(record.celery_task_id, terminate=True, signal="SIGTERM")
                except Exception as exc:
                    logger.warning("Could not revoke Celery task %s: %s", record.celery_task_id, exc)
            record.status = TaskRecord.Status.REVOKED
            record.completed_at = timezone.now()
            record.save(update_fields=["status", "completed_at", "updated_at"])
            revoked += 1

        self.message_user(request, f"{revoked} task(s) revoked.", messages.SUCCESS)
