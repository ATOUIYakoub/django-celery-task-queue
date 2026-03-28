"""
TaskRecord model — persists every submitted Celery job in PostgreSQL.
"""

from uuid import uuid4

from django.db import models


class TaskRecord(models.Model):
    # -----------------------------------------------------------------------
    # Status choices
    # -----------------------------------------------------------------------
    class Status(models.TextChoices):
        PENDING  = "pending",  "Pending"
        SENT     = "sent",     "Sent"
        STARTED  = "started",  "Started"
        PROGRESS = "progress", "In Progress"
        SUCCESS  = "success",  "Success"
        FAILURE  = "failure",  "Failure"
        RETRY    = "retry",    "Retry"
        REVOKED  = "revoked",  "Revoked"

    # -----------------------------------------------------------------------
    # Task type choices
    # -----------------------------------------------------------------------
    class TaskType(models.TextChoices):
        FILE_PROCESSING = "file_processing", "File Processing"
        DATA_EXPORT     = "data_export",     "Data Export"
        EMAIL_BATCH     = "email_batch",     "Email Batch"

    # -----------------------------------------------------------------------
    # Priority choices  (maps to kombu Queue priorities)
    # -----------------------------------------------------------------------
    class Priority(models.IntegerChoices):
        LOW     = 1,  "Low"
        DEFAULT = 5,  "Default"
        HIGH    = 10, "High"

    # -----------------------------------------------------------------------
    # Fields
    # -----------------------------------------------------------------------
    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)
    name = models.CharField(max_length=200)
    task_type = models.CharField(
        max_length=30,
        choices=TaskType.choices,
    )
    priority = models.IntegerField(
        choices=Priority.choices,
        default=Priority.DEFAULT,
    )
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.PENDING,
        db_index=True,
    )
    celery_task_id = models.CharField(max_length=255, blank=True)

    # Progress tracking
    progress = models.IntegerField(default=0)  # 0–100
    progress_message = models.CharField(max_length=500, blank=True)

    # Payload / result
    payload = models.JSONField(default=dict)
    result  = models.JSONField(null=True, blank=True)

    # Error handling
    error_message = models.TextField(blank=True)
    retry_count   = models.IntegerField(default=0)
    max_retries   = models.IntegerField(default=3)

    # Timestamps
    created_at   = models.DateTimeField(auto_now_add=True)
    updated_at   = models.DateTimeField(auto_now=True)
    started_at   = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------
    def duration_seconds(self) -> float | None:
        """Return elapsed seconds between start and completion, or None."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    # -----------------------------------------------------------------------
    # Meta
    # -----------------------------------------------------------------------
    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["status"],     name="taskrecord_status_idx"),
            models.Index(fields=["task_type"],  name="taskrecord_task_type_idx"),
            models.Index(fields=["priority"],   name="taskrecord_priority_idx"),
            models.Index(fields=["created_at"], name="taskrecord_created_at_idx"),
        ]
        verbose_name = "Task Record"
        verbose_name_plural = "Task Records"

    def __str__(self) -> str:
        return f"{self.name} [{self.status}]"
