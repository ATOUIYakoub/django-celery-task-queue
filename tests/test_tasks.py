"""
Unit tests for Celery task functions and DLQ utilities.

Uses unittest.mock to avoid needing a live Celery broker.
DB access uses pytest-django's `db` fixture.
"""

import json
from unittest.mock import MagicMock, call, patch

import pytest

from apps.tasks.models import TaskRecord
from apps.tasks.tasks import requeue_from_dlq, update_task_progress
from tests.conftest import TaskRecordFactory


# ---------------------------------------------------------------------------
# update_task_progress
# ---------------------------------------------------------------------------
class TestUpdateTaskProgress:

    def test_updates_db_record(self, db):
        """update_task_progress writes progress and message to the DB."""
        record = TaskRecordFactory(progress=0, progress_message="")

        with patch("apps.tasks.tasks.get_channel_layer") as mock_layer:
            mock_layer.return_value = None  # Skip channel layer push

            update_task_progress(str(record.id), 50, "Halfway there")

        record.refresh_from_db()
        assert record.progress == 50
        assert record.progress_message == "Halfway there"
        assert record.status == TaskRecord.Status.PROGRESS

    def test_sends_channel_layer_group_message(self, db):
        """update_task_progress calls group_send on the channel layer."""
        record = TaskRecordFactory()
        mock_layer = MagicMock()

        with patch("apps.tasks.tasks.get_channel_layer", return_value=mock_layer):
            with patch("apps.tasks.tasks.async_to_sync") as mock_async:
                mock_send = MagicMock()
                mock_async.return_value = mock_send

                update_task_progress(str(record.id), 75, "Almost done")

                mock_async.assert_called_once_with(mock_layer.group_send)
                mock_send.assert_called_once_with(
                    f"task_{record.id}",
                    {
                        "type": "task.progress",
                        "task_id": str(record.id),
                        "status": "progress",
                        "progress": 75,
                        "message": "Almost done",
                    },
                )

    def test_handles_missing_channel_layer_gracefully(self, db):
        """update_task_progress does not raise if channel layer is None."""
        record = TaskRecordFactory()

        with patch("apps.tasks.tasks.get_channel_layer", return_value=None):
            # Should not raise
            update_task_progress(str(record.id), 10, "Starting")

        record.refresh_from_db()
        assert record.progress == 10


# ---------------------------------------------------------------------------
# process_file_task
# ---------------------------------------------------------------------------
class TestProcessFileTask:

    @patch("apps.tasks.tasks.update_task_progress")
    @patch("apps.tasks.tasks.time.sleep", return_value=None)  # Skip actual sleeps
    def test_calls_update_task_progress_10_times(self, mock_sleep, mock_progress, db):
        """process_file_task calls update_task_progress once per chunk (10 total)."""
        from apps.tasks.tasks import process_file_task

        record = TaskRecordFactory()
        mock_self = MagicMock()
        mock_self.request.id = "test-celery-id"

        with patch.object(
            process_file_task,
            "apply_async",
            wraps=lambda *a, **kw: None,
        ):
            # Call the underlying function directly (skip Celery machinery)
            process_file_task.__wrapped__ = None  # Ensure it runs synchronously
            result = process_file_task.run(
                mock_self,
                str(record.id),
                {"priority": "default"},
            )

        assert mock_progress.call_count == 10
        assert result is not None
        assert "rows_processed" in result
        assert "output_path" in result

    @patch("apps.tasks.tasks.update_task_progress")
    @patch("apps.tasks.tasks.time.sleep", return_value=None)
    def test_updates_task_record_status_to_started(self, mock_sleep, mock_progress, db):
        """process_file_task marks the record as STARTED at the beginning."""
        from apps.tasks.tasks import process_file_task

        record = TaskRecordFactory(status=TaskRecord.Status.PENDING)
        mock_self = MagicMock()
        mock_self.request.id = "test-celery-id-2"

        process_file_task.run(mock_self, str(record.id), {})

        record.refresh_from_db()
        # on_success is wired via the base class; the task itself sets STARTED
        assert record.started_at is not None


# ---------------------------------------------------------------------------
# DLQ signal + requeue_from_dlq
# ---------------------------------------------------------------------------
class TestDLQSignal:

    def test_dlq_entry_added_to_redis_on_max_retries_exceeded(
        self, db, redis_connection
    ):
        """
        Simulates calling the task_failure signal handler directly with
        retries == max_retries, asserts the entry is pushed to dlq:tasks.
        """
        from config.celery import on_task_failure

        record = TaskRecordFactory(status=TaskRecord.Status.FAILURE)

        mock_sender = MagicMock()
        mock_sender.name = "apps.tasks.tasks.process_file_task"
        mock_sender.max_retries = 3

        mock_request = MagicMock()
        mock_request.retries = 3  # == max_retries → should DLQ

        on_task_failure(
            sender=mock_sender,
            task_id="dead-celery-id",
            exception=Exception("boom"),
            args=[str(record.id), {}],
            kwargs={},
            traceback=None,
            einfo=None,
            request=mock_request,
        )

        dlq_entries = redis_connection.lrange("dlq:tasks", 0, -1)
        assert len(dlq_entries) >= 1
        entry = json.loads(dlq_entries[0])
        assert entry["task_id"] == "dead-celery-id"
        assert entry["task_name"] == "apps.tasks.tasks.process_file_task"

    def test_requeue_from_dlq_resubmits_and_removes_entry(
        self, db, redis_connection
    ):
        """requeue_from_dlq picks the entry, re-submits, and removes it from Redis."""
        record = TaskRecordFactory(status=TaskRecord.Status.FAILURE)

        # Manually seed the DLQ
        dlq_entry = json.dumps(
            {
                "task_id": "dead-celery-id-2",
                "task_name": "apps.tasks.tasks.process_file_task",
                "args": [str(record.id), {"priority": "default"}],
                "kwargs": {},
                "exception": "Test error",
                "retries": 3,
            }
        )
        redis_connection.lpush("dlq:tasks", dlq_entry)

        with patch("apps.tasks.tasks.process_file_task.apply_async") as mock_apply:
            result = requeue_from_dlq(str(record.id))

        assert result is True
        mock_apply.assert_called_once()

        record.refresh_from_db()
        assert record.status == TaskRecord.Status.PENDING
        assert record.retry_count == 0

        # Entry should be removed from Redis
        remaining = redis_connection.lrange("dlq:tasks", 0, -1)
        entries = [json.loads(e) for e in remaining]
        assert not any(e["task_id"] == "dead-celery-id-2" for e in entries)

    def test_requeue_from_dlq_returns_false_when_not_found(
        self, db, redis_connection
    ):
        """requeue_from_dlq returns False for a task_id not in the DLQ."""
        result = requeue_from_dlq("00000000-0000-0000-0000-000000000000")
        assert result is False
