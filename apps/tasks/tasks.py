"""
Celery tasks for the taskqueue project.

Contains:
  - TaskWithDB          — base Task class wiring lifecycle signals to TaskRecord
  - update_task_progress — helper that writes progress to DB + pushes via Channel Layer
  - process_file_task   — file processing simulation (queue: default/high)
  - export_data_task    — data export simulation (queue: low)
  - send_email_batch_task — email batch simulation (queue: high)
  - requeue_from_dlq    — utility to re-submit a task from the Redis DLQ list
"""

import json
import logging
import random
import time
from datetime import timezone as dt_timezone
from uuid import uuid4

import redis as redis_module
from asgiref.sync import async_to_sync
from celery import Task
from celery.exceptions import MaxRetriesExceededError
from channels.layers import get_channel_layer
from django.conf import settings
from django.utils import timezone

from .models import TaskRecord

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Base task class
# ---------------------------------------------------------------------------
class TaskWithDB(Task):
    """
    Custom Celery base Task that keeps TaskRecord in sync with task lifecycle.
    Subclasses should set `abstract = True` unless they are concrete tasks.
    """

    abstract = True

    def on_success(self, retval, task_id, args, kwargs):
        """Mark the TaskRecord as success and record completion time."""
        task_record_id = args[0] if args else kwargs.get("task_record_id")
        if task_record_id is None:
            return
        try:
            record = TaskRecord.objects.get(pk=task_record_id)
            record.status = TaskRecord.Status.SUCCESS
            record.completed_at = timezone.now()
            record.progress = 100
            record.save(update_fields=["status", "completed_at", "progress", "updated_at"])
            logger.info("Task %s completed successfully.", task_id)
        except TaskRecord.DoesNotExist:
            logger.warning("TaskRecord %s not found in on_success.", task_record_id)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Mark the TaskRecord as failure and store the error message."""
        task_record_id = args[0] if args else kwargs.get("task_record_id")
        if task_record_id is None:
            return
        try:
            record = TaskRecord.objects.get(pk=task_record_id)
            record.status = TaskRecord.Status.FAILURE
            record.error_message = str(exc)
            record.completed_at = timezone.now()
            record.save(update_fields=["status", "error_message", "completed_at", "updated_at"])
            logger.error("Task %s failed: %s", task_id, exc)
        except TaskRecord.DoesNotExist:
            logger.warning("TaskRecord %s not found in on_failure.", task_record_id)

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        """Increment retry_count and set status to 'retry'."""
        task_record_id = args[0] if args else kwargs.get("task_record_id")
        if task_record_id is None:
            return
        try:
            record = TaskRecord.objects.get(pk=task_record_id)
            record.status = TaskRecord.Status.RETRY
            record.retry_count = (record.retry_count or 0) + 1
            record.save(update_fields=["status", "retry_count", "updated_at"])
            logger.warning("Task %s retrying (attempt %d): %s", task_id, record.retry_count, exc)
        except TaskRecord.DoesNotExist:
            logger.warning("TaskRecord %s not found in on_retry.", task_record_id)


# ---------------------------------------------------------------------------
# Progress helper
# ---------------------------------------------------------------------------
def update_task_progress(task_record_id: str, percent: int, message: str) -> None:
    """
    1. Update TaskRecord.progress and progress_message in PostgreSQL.
    2. Push a 'task.progress' event to the Channel Layer group for that task,
       so any connected WebSocket clients receive the update in real time.
    """
    try:
        TaskRecord.objects.filter(pk=task_record_id).update(
            status=TaskRecord.Status.PROGRESS,
            progress=percent,
            progress_message=message,
        )
    except Exception as exc:
        logger.error("Failed to update TaskRecord %s progress: %s", task_record_id, exc)

    channel_layer = get_channel_layer()
    if channel_layer is None:
        logger.warning("No channel layer configured — skipping WebSocket push.")
        return

    group_name = f"task_{task_record_id}"
    event = {
        "type": "task.progress",
        "task_id": str(task_record_id),
        "status": "progress",
        "progress": percent,
        "message": message,
    }
    try:
        async_to_sync(channel_layer.group_send)(group_name, event)
    except Exception as exc:
        logger.error("Channel layer group_send failed for %s: %s", group_name, exc)


# ---------------------------------------------------------------------------
# Task 1 — File Processing
# ---------------------------------------------------------------------------
from config.celery import app  # noqa: E402  (import after Django init)


@app.task(
    bind=True,
    base=TaskWithDB,
    name="apps.tasks.tasks.process_file_task",
    queue="default",
    autoretry_for=(Exception,),
    max_retries=3,
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
)
def process_file_task(self, task_record_id: str, file_config: dict):
    """
    Simulate a multi-step file processing job.
    Steps: 10 chunks, each sleeping 0.5–1.5 s.
    Progress is reported after each chunk via update_task_progress.
    """
    try:
        record = TaskRecord.objects.get(pk=task_record_id)
        record.status = TaskRecord.Status.STARTED
        record.started_at = timezone.now()
        record.celery_task_id = self.request.id
        record.save(update_fields=["status", "started_at", "celery_task_id", "updated_at"])
    except TaskRecord.DoesNotExist:
        logger.error("process_file_task: TaskRecord %s not found.", task_record_id)
        return

    total_steps = 10
    for step in range(1, total_steps + 1):
        time.sleep(random.uniform(0.5, 1.5))
        percent = int((step / total_steps) * 100)
        message = f"Processing chunk {step}/{total_steps}…"
        update_task_progress(task_record_id, percent, message)
        logger.debug("process_file_task [%s] step %d/%d", task_record_id, step, total_steps)

    result = {
        "rows_processed": random.randint(1000, 50000),
        "output_path": f"/exports/file_{task_record_id}.csv",
    }
    TaskRecord.objects.filter(pk=task_record_id).update(result=result)
    return result


# ---------------------------------------------------------------------------
# Task 2 — Data Export
# ---------------------------------------------------------------------------
@app.task(
    bind=True,
    base=TaskWithDB,
    name="apps.tasks.tasks.export_data_task",
    queue="low",
    autoretry_for=(redis_module.exceptions.ConnectionError,),
    max_retries=5,
    retry_backoff=True,
)
def export_data_task(self, task_record_id: str, export_config: dict):
    """
    Simulate a paginated data export job.
    Phase 1: DB query (2 s). Phase 2: 5 pages, 1 s each.
    Progress steps: 0 → 20 → 40 → 60 → 80 → 100.
    """
    try:
        record = TaskRecord.objects.get(pk=task_record_id)
        record.status = TaskRecord.Status.STARTED
        record.started_at = timezone.now()
        record.celery_task_id = self.request.id
        record.save(update_fields=["status", "started_at", "celery_task_id", "updated_at"])
    except TaskRecord.DoesNotExist:
        logger.error("export_data_task: TaskRecord %s not found.", task_record_id)
        return

    # Phase 1 — simulate DB query
    update_task_progress(task_record_id, 0, "Querying database…")
    time.sleep(2)
    update_task_progress(task_record_id, 20, "Query complete, starting export…")

    # Phase 2 — paginated export (5 pages)
    pages = 5
    for page in range(1, pages + 1):
        time.sleep(1)
        percent = 20 + int((page / pages) * 80)
        message = f"Exporting page {page}/{pages}…"
        update_task_progress(task_record_id, percent, message)
        logger.debug("export_data_task [%s] page %d/%d", task_record_id, page, pages)

    export_format = export_config.get("format", "csv")
    result = {
        "total_records": random.randint(500, 10000),
        "format": export_format,
        "download_url": f"/downloads/{task_record_id}.zip",
    }
    TaskRecord.objects.filter(pk=task_record_id).update(result=result)
    return result


# ---------------------------------------------------------------------------
# Task 3 — Email Batch
# ---------------------------------------------------------------------------
@app.task(
    bind=True,
    base=TaskWithDB,
    name="apps.tasks.tasks.send_email_batch_task",
    queue="high",
    autoretry_for=(Exception,),
    max_retries=2,
    retry_backoff=True,
)
def send_email_batch_task(self, task_record_id: str, batch_config: dict):
    """
    Simulate sending N emails in batches of 10.
    Sleeps 0.2 s per batch and reports progress after each.
    """
    try:
        record = TaskRecord.objects.get(pk=task_record_id)
        record.status = TaskRecord.Status.STARTED
        record.started_at = timezone.now()
        record.celery_task_id = self.request.id
        record.save(update_fields=["status", "started_at", "celery_task_id", "updated_at"])
    except TaskRecord.DoesNotExist:
        logger.error("send_email_batch_task: TaskRecord %s not found.", task_record_id)
        return

    total_emails = batch_config.get("count", 100)
    batch_size = 10
    total_batches = max(1, (total_emails + batch_size - 1) // batch_size)
    sent = 0

    for batch_num in range(1, total_batches + 1):
        emails_in_batch = min(batch_size, total_emails - sent)
        time.sleep(0.2)
        sent += emails_in_batch
        percent = int((sent / total_emails) * 100)
        message = f"Sent {sent}/{total_emails} emails (batch {batch_num}/{total_batches})"
        update_task_progress(task_record_id, percent, message)
        logger.debug(
            "send_email_batch_task [%s] batch %d/%d sent=%d",
            task_record_id, batch_num, total_batches, sent,
        )

    result = {
        "sent": sent,
        "failed": 0,
        "batch_id": str(uuid4()),
    }
    TaskRecord.objects.filter(pk=task_record_id).update(result=result)
    return result


# ---------------------------------------------------------------------------
# DLQ utility — re-queue a task from the Redis DLQ list
# ---------------------------------------------------------------------------
def requeue_from_dlq(task_record_id: str) -> bool:
    """
    Fetch the original task details for task_record_id from the Redis DLQ list,
    re-submit it to its original queue with a fresh retry count,
    and update the TaskRecord status back to 'pending'.

    Returns True if successfully requeued, False otherwise.
    """
    _TASK_MAP = {
        "apps.tasks.tasks.process_file_task": process_file_task,
        "apps.tasks.tasks.export_data_task": export_data_task,
        "apps.tasks.tasks.send_email_batch_task": send_email_batch_task,
    }

    r = redis_module.from_url(settings.CELERY_BROKER_URL)
    dlq_key = "dlq:tasks"
    raw_entries = r.lrange(dlq_key, 0, -1)

    target_entry = None
    target_raw = None
    for raw in raw_entries:
        entry = json.loads(raw)
        if entry.get("args") and str(entry["args"][0]) == str(task_record_id):
            target_entry = entry
            target_raw = raw
            break

    if target_entry is None:
        logger.warning("requeue_from_dlq: No DLQ entry found for task_record_id %s.", task_record_id)
        return False

    task_name = target_entry["task_name"]
    celery_task_fn = _TASK_MAP.get(task_name)
    if celery_task_fn is None:
        logger.error("requeue_from_dlq: Unknown task name '%s'.", task_name)
        return False

    original_args = target_entry.get("args", [])
    original_kwargs = target_entry.get("kwargs", {})

    # Reset TaskRecord
    try:
        record = TaskRecord.objects.get(pk=task_record_id)
        record.status = TaskRecord.Status.PENDING
        record.retry_count = 0
        record.error_message = ""
        record.save(update_fields=["status", "retry_count", "error_message", "updated_at"])
    except TaskRecord.DoesNotExist:
        logger.warning("requeue_from_dlq: TaskRecord %s not found.", task_record_id)
        return False

    # Re-submit to original queue
    celery_task_fn.apply_async(args=original_args, kwargs=original_kwargs)

    # Remove from DLQ
    r.lrem(dlq_key, 1, target_raw)

    logger.info(
        "requeue_from_dlq: Task %s re-submitted as '%s' and removed from DLQ.",
        task_record_id,
        task_name,
    )
    return True
