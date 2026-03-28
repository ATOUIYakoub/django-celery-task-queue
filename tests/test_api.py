"""
API integration tests for the tasks app.

All tests use pytest-django (db fixture) and DRF APIClient.
Celery tasks are run eagerly (CELERY_TASK_ALWAYS_EAGER=True applied per-test).
"""

import pytest
from unittest.mock import patch, MagicMock

from rest_framework import status

from apps.tasks.models import TaskRecord
from tests.conftest import TaskRecordFactory


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
SUBMIT_URL   = "/api/tasks/submit/"
LIST_URL     = "/api/tasks/"
STATS_URL    = "/api/tasks/stats/"


def _task_detail_url(pk) -> str:
    return f"/api/tasks/{pk}/"


def _cancel_url(pk) -> str:
    return f"/api/tasks/{pk}/cancel/"


def _retry_url(pk) -> str:
    return f"/api/tasks/{pk}/retry/"


# ---------------------------------------------------------------------------
# POST /api/tasks/submit/
# ---------------------------------------------------------------------------
class TestTaskSubmit:

    @patch("apps.tasks.views._dispatch_celery_task", return_value="mock-celery-id-001")
    def test_submit_valid_file_processing(self, mock_dispatch, api_client, db):
        """Valid payload → 201, TaskRecord created, celery_task_id populated."""
        payload = {
            "name": "My File Job",
            "task_type": "file_processing",
            "priority": 5,
            "payload": {"source": "/data/file.csv"},
        }
        response = api_client.post(SUBMIT_URL, payload, format="json")

        assert response.status_code == status.HTTP_201_CREATED
        data = response.json()
        assert "id" in data
        assert data["celery_task_id"] == "mock-celery-id-001"
        assert "ws_url" in data
        assert "/ws/tasks/" in data["ws_url"]

        record = TaskRecord.objects.get(pk=data["id"])
        assert record.task_type == "file_processing"
        assert record.celery_task_id == "mock-celery-id-001"

    @patch("apps.tasks.views._dispatch_celery_task", return_value="mock-celery-id-002")
    def test_submit_valid_email_batch(self, mock_dispatch, api_client, db):
        """Email batch job with high priority → 201."""
        payload = {
            "name": "Email Campaign",
            "task_type": "email_batch",
            "priority": 10,
            "payload": {"count": 200, "template": "welcome"},
        }
        response = api_client.post(SUBMIT_URL, payload, format="json")
        assert response.status_code == status.HTTP_201_CREATED

    def test_submit_invalid_task_type(self, api_client, db):
        """Unknown task_type → 400 Bad Request."""
        payload = {
            "name": "Bad Job",
            "task_type": "unknown_type",
            "priority": 5,
            "payload": {"key": "value"},
        }
        response = api_client.post(SUBMIT_URL, payload, format="json")
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "task_type" in response.json()

    def test_submit_invalid_priority(self, api_client, db):
        """Non-allowed priority value → 400 Bad Request."""
        payload = {
            "name": "Bad Priority Job",
            "task_type": "file_processing",
            "priority": 99,
            "payload": {"key": "value"},
        }
        response = api_client.post(SUBMIT_URL, payload, format="json")
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_submit_empty_payload(self, api_client, db):
        """Empty payload dict → 400 Bad Request."""
        payload = {
            "name": "Empty Payload Job",
            "task_type": "file_processing",
            "priority": 5,
            "payload": {},
        }
        response = api_client.post(SUBMIT_URL, payload, format="json")
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_submit_missing_name(self, api_client, db):
        """Missing required 'name' field → 400 Bad Request."""
        payload = {
            "task_type": "data_export",
            "priority": 1,
            "payload": {"format": "csv"},
        }
        response = api_client.post(SUBMIT_URL, payload, format="json")
        assert response.status_code == status.HTTP_400_BAD_REQUEST


# ---------------------------------------------------------------------------
# GET /api/tasks/
# ---------------------------------------------------------------------------
class TestTaskList:

    def test_list_returns_200(self, api_client, sample_task):
        """GET list → 200, returns a list."""
        response = api_client.get(LIST_URL)
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        # DRF DefaultRouter returns a list or paginated dict
        if isinstance(data, dict):
            results = data.get("results", data)
        else:
            results = data
        assert isinstance(results, list)
        assert len(results) >= 1

    def test_list_filter_by_status(self, api_client, db):
        """?status=pending filter returns only pending tasks."""
        TaskRecordFactory(status=TaskRecord.Status.PENDING)
        TaskRecordFactory(status=TaskRecord.Status.SUCCESS)
        response = api_client.get(LIST_URL + "?status=pending")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        results = data.get("results", data) if isinstance(data, dict) else data
        assert all(r["status"] == "pending" for r in results)

    def test_list_filter_by_task_type(self, api_client, db):
        """?task_type=email_batch filter returns only email tasks."""
        TaskRecordFactory(task_type=TaskRecord.TaskType.EMAIL_BATCH)
        TaskRecordFactory(task_type=TaskRecord.TaskType.DATA_EXPORT)
        response = api_client.get(LIST_URL + "?task_type=email_batch")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        results = data.get("results", data) if isinstance(data, dict) else data
        assert all(r["task_type"] == "email_batch" for r in results)

    def test_list_filter_by_priority(self, api_client, db):
        """?priority=10 filter returns only high-priority tasks."""
        TaskRecordFactory(priority=TaskRecord.Priority.HIGH)
        TaskRecordFactory(priority=TaskRecord.Priority.LOW)
        response = api_client.get(LIST_URL + "?priority=10")
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        results = data.get("results", data) if isinstance(data, dict) else data
        assert all(r["priority"] == 10 for r in results)


# ---------------------------------------------------------------------------
# GET /api/tasks/{id}/
# ---------------------------------------------------------------------------
class TestTaskRetrieve:

    def test_retrieve_returns_200(self, api_client, sample_task):
        """GET single task → 200 with correct fields."""
        response = api_client.get(_task_detail_url(sample_task.id))
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert data["id"] == str(sample_task.id)
        assert data["name"] == sample_task.name
        assert data["task_type"] == sample_task.task_type
        assert "duration_seconds" in data

    def test_retrieve_unknown_id_returns_404(self, api_client, db):
        """Non-existent UUID → 404."""
        response = api_client.get(_task_detail_url("00000000-0000-0000-0000-000000000000"))
        assert response.status_code == status.HTTP_404_NOT_FOUND


# ---------------------------------------------------------------------------
# GET /api/tasks/stats/
# ---------------------------------------------------------------------------
class TestTaskStats:

    @patch("apps.tasks.views.redis_module")
    def test_stats_returns_200(self, mock_redis, api_client, sample_task):
        """GET stats → 200 with required keys."""
        mock_redis.from_url.return_value.llen.return_value = 0
        response = api_client.get(STATS_URL)
        assert response.status_code == status.HTTP_200_OK
        data = response.json()
        assert "total" in data
        assert "by_status" in data
        assert "by_type" in data
        assert "dlq_count" in data
        assert data["total"] >= 1


# ---------------------------------------------------------------------------
# POST /api/tasks/{id}/cancel/
# ---------------------------------------------------------------------------
class TestTaskCancel:

    @patch("apps.tasks.views.get_channel_layer")
    @patch("apps.tasks.views.async_to_sync")
    def test_cancel_sets_revoked(self, mock_async, mock_layer, api_client, sample_task):
        """Cancel endpoint → status becomes 'revoked'."""
        mock_layer.return_value = MagicMock()
        mock_async.return_value = MagicMock()

        with patch("apps.tasks.views.Control") as mock_control:
            mock_control.return_value.revoke = MagicMock()
            response = api_client.post(_cancel_url(sample_task.id))

        assert response.status_code == status.HTTP_200_OK
        sample_task.refresh_from_db()
        assert sample_task.status == TaskRecord.Status.REVOKED


# ---------------------------------------------------------------------------
# POST /api/tasks/{id}/retry/
# ---------------------------------------------------------------------------
class TestTaskRetry:

    @patch("apps.tasks.views._dispatch_celery_task", return_value="retry-celery-id")
    def test_retry_failed_task_creates_new_record(self, mock_dispatch, api_client, failed_task):
        """Retry on a failed task → 201, new TaskRecord created."""
        initial_count = TaskRecord.objects.count()
        response = api_client.post(_retry_url(failed_task.id))

        assert response.status_code == status.HTTP_201_CREATED
        data = response.json()
        assert "new_id" in data
        assert data["new_id"] != str(failed_task.id)
        assert TaskRecord.objects.count() == initial_count + 1

    def test_retry_pending_task_returns_400(self, api_client, sample_task):
        """Retry on a non-failed task → 400 Bad Request."""
        response = api_client.post(_retry_url(sample_task.id))
        assert response.status_code == status.HTTP_400_BAD_REQUEST
