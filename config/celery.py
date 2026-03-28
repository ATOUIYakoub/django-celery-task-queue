"""
Celery application instance for the taskqueue project.

This module is imported by Django's __init__.py-level app registry trick
(see config/__init__.py) to ensure the Celery app is always loaded when
Django starts, so shared_task decorators work correctly.
"""

import logging
import os

from celery import Celery
from celery.signals import task_failure, worker_ready

logger = logging.getLogger(__name__)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings.development")

app = Celery("taskqueue")

# Pull all CELERY_* settings from Django settings namespace
app.config_from_object("django.conf:settings", namespace="CELERY")

# Auto-discover tasks.py in every INSTALLED_APP
app.autodiscover_tasks()


@app.on_after_configure.connect
def on_celery_configured(sender, **kwargs):
    """Log a startup message once Celery has finished reading configuration."""
    logger.info(
        "Celery configured — broker: %s",
        sender.conf.broker_url,
    )


@worker_ready.connect
def on_worker_ready(sender, **kwargs):
    logger.info("Celery worker is ready and consuming queues.")


# ---------------------------------------------------------------------------
# Dead Letter Queue signal — wired here so it is always active.
# Full DLQ logic lives in Phase 5; this import is deferred to avoid circular
# imports during startup (apps aren't loaded yet at module-parse time).
# ---------------------------------------------------------------------------
@task_failure.connect
def on_task_failure(sender, task_id, exception, args, kwargs, traceback, einfo, **kw):
    """
    When a task exhausts its retries, push it onto the Redis DLQ list
    and update the TaskRecord status to 'failure'.
    """
    import json

    import redis as redis_module
    from django.conf import settings

    # Only DLQ tasks that have no retries left (request.retries == max_retries)
    request = kw.get("request") or getattr(sender, "request", None)
    if request is None:
        return

    retries = getattr(request, "retries", 0)
    max_retries = getattr(sender, "max_retries", None)

    if max_retries is not None and retries < max_retries:
        # Still has retry attempts remaining – let Celery handle it
        return

    try:
        r = redis_module.from_url(settings.CELERY_BROKER_URL)
        dlq_entry = json.dumps(
            {
                "task_id": task_id,
                "task_name": sender.name,
                "args": list(args),
                "kwargs": kwargs,
                "exception": str(exception),
                "retries": retries,
            }
        )
        r.lpush("dlq:tasks", dlq_entry)
        logger.warning(
            "Task %s (%s) pushed to DLQ after %d retries.",
            sender.name,
            task_id,
            retries,
        )
    except Exception as exc:
        logger.error("Failed to push task %s to DLQ: %s", task_id, exc)
