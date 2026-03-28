"""
Shared pytest fixtures for the taskqueue test suite.

Provides:
  - api_client       — DRF APIClient
  - sample_task      — a persisted TaskRecord (using factory-boy factory)
  - redis_connection — direct redis-py client pointed at the broker DB
"""

import factory
import pytest
import redis as redis_module
from django.conf import settings
from rest_framework.test import APIClient

from apps.tasks.models import TaskRecord


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------
class TaskRecordFactory(factory.django.DjangoModelFactory):
    """Factory that generates valid TaskRecord instances for tests."""

    class Meta:
        model = TaskRecord

    name = factory.Sequence(lambda n: f"Test Task {n}")
    task_type = TaskRecord.TaskType.FILE_PROCESSING
    priority = TaskRecord.Priority.DEFAULT
    status = TaskRecord.Status.PENDING
    payload = factory.LazyFunction(lambda: {"source": "test", "size": 100})
    celery_task_id = factory.Sequence(lambda n: f"celery-task-{n:04d}")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def api_client() -> APIClient:
    """Un-authenticated DRF test client."""
    return APIClient()


@pytest.fixture
def sample_task(db) -> TaskRecord:
    """A persisted TaskRecord in the PENDING state."""
    return TaskRecordFactory()


@pytest.fixture
def failed_task(db) -> TaskRecord:
    """A persisted TaskRecord in the FAILURE state (eligible for retry)."""
    return TaskRecordFactory(
        status=TaskRecord.Status.FAILURE,
        error_message="Simulated failure",
    )


@pytest.fixture
def redis_connection():
    """Direct redis-py client for asserting Redis state in tests."""
    client = redis_module.from_url(settings.CELERY_BROKER_URL)
    yield client
    # Cleanup DLQ entries created during tests
    client.delete("dlq:tasks")
