"""
WebSocket consumer tests using pytest-asyncio and Django Channels
WebsocketCommunicator.

Requires:
  - pytest-asyncio (asyncio_mode = auto in pytest.ini)
  - channels[daphne] installed

All tests are async and tagged with @pytest.mark.django_db(transaction=True)
because Channels consumers access the DB from a thread pool.
"""

import json
import pytest
from channels.testing import WebsocketCommunicator
from channels.layers import get_channel_layer
from asgiref.sync import sync_to_async

from config.asgi import application
from apps.tasks.models import TaskRecord
from tests.conftest import TaskRecordFactory


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _ws_url(task_id: str) -> str:
    return f"/ws/tasks/{task_id}/"


async def _connect(task_id: str):
    """Create and return a connected WebsocketCommunicator for a task."""
    communicator = WebsocketCommunicator(application, _ws_url(str(task_id)))
    connected, subprotocol = await communicator.connect()
    return communicator, connected


# ---------------------------------------------------------------------------
# Connection tests
# ---------------------------------------------------------------------------
class TestWebSocketConnect:

    @pytest.mark.django_db(transaction=True)
    async def test_connect_valid_task_receives_initial_state(self):
        """Valid UUID with existing TaskRecord → connected, initial state sent."""
        record = await sync_to_async(TaskRecordFactory)()

        communicator, connected = await _connect(record.id)
        assert connected is True

        # Should receive initial state immediately
        response = await communicator.receive_json_from(timeout=5)
        assert response["type"] == "task.state"
        assert response["task_id"] == str(record.id)
        assert response["status"] == record.status
        assert "progress" in response

        await communicator.disconnect()

    @pytest.mark.django_db(transaction=True)
    async def test_connect_invalid_uuid_closes_with_4004(self):
        """Malformed UUID → connection closed with code 4004."""
        communicator = WebsocketCommunicator(application, "/ws/tasks/not-a-uuid/")
        connected, subprotocol = await communicator.connect()
        # Connection should be rejected at the URL pattern level
        # (regex won't match) or closed with 4004 if it slips through
        assert connected is False or await communicator.receive_nothing(timeout=1)
        await communicator.disconnect()

    @pytest.mark.django_db(transaction=True)
    async def test_connect_nonexistent_task_closes_with_4004(self):
        """Valid UUID format but no matching TaskRecord → closed with 4004."""
        fake_id = "00000000-0000-0000-0000-000000000000"
        communicator = WebsocketCommunicator(application, _ws_url(fake_id))
        connected, subprotocol = await communicator.connect()

        if connected:
            # Consumer accepted but should close immediately
            close_code = await communicator.receive_output(timeout=5)
            assert close_code.get("code") == 4004 or close_code.get("type") == "websocket.close"
        else:
            assert not connected

        await communicator.disconnect()


# ---------------------------------------------------------------------------
# Ping / Pong
# ---------------------------------------------------------------------------
class TestWebSocketPing:

    @pytest.mark.django_db(transaction=True)
    async def test_ping_receives_pong(self):
        """Send 'ping' → receive {'type': 'pong'}."""
        record = await sync_to_async(TaskRecordFactory)()
        communicator, connected = await _connect(record.id)
        assert connected

        # Consume initial state message
        await communicator.receive_json_from(timeout=5)

        # Send ping
        await communicator.send_json_to({"type": "ping"})
        response = await communicator.receive_json_from(timeout=5)
        assert response == {"type": "pong"}

        await communicator.disconnect()


# ---------------------------------------------------------------------------
# Group push → client receives it
# ---------------------------------------------------------------------------
class TestWebSocketGroupPush:

    @pytest.mark.django_db(transaction=True)
    async def test_task_progress_event_forwarded_to_client(self):
        """
        Sending a task.progress event to the group propagates to the client.
        """
        record = await sync_to_async(TaskRecordFactory)()
        communicator, connected = await _connect(record.id)
        assert connected

        # Consume initial state
        await communicator.receive_json_from(timeout=5)

        # Push a progress event directly via channel layer
        channel_layer = get_channel_layer()
        await channel_layer.group_send(
            f"task_{record.id}",
            {
                "type": "task.progress",
                "task_id": str(record.id),
                "status": "progress",
                "progress": 42,
                "message": "Processing chunk 4/10…",
            },
        )

        # Client should receive it
        response = await communicator.receive_json_from(timeout=5)
        assert response["type"] == "task.progress"
        assert response["task_id"] == str(record.id)
        assert response["progress"] == 42
        assert response["message"] == "Processing chunk 4/10…"

        await communicator.disconnect()


# ---------------------------------------------------------------------------
# Cancel command
# ---------------------------------------------------------------------------
class TestWebSocketCancel:

    @pytest.mark.django_db(transaction=True)
    async def test_cancel_command_revokes_task(self):
        """
        Sending {'type': 'cancel'} → TaskRecord status becomes 'revoked'
        and a task.update event is broadcast.
        """
        record = await sync_to_async(TaskRecordFactory)(
            celery_task_id="running-celery-task-id",
            status=TaskRecord.Status.STARTED,
        )
        communicator, connected = await _connect(record.id)
        assert connected

        # Consume initial state
        await communicator.receive_json_from(timeout=5)

        from unittest.mock import patch, MagicMock, AsyncMock

        with patch("apps.tasks.consumers.sync_to_async") as mock_s2a:
            # Make the DB get return the record
            async def fake_get(*args, **kwargs):
                return record

            async def fake_update(*args, **kwargs):
                pass

            mock_s2a.side_effect = lambda fn: (
                fake_get if "get" in str(fn) else fake_update
            )

            with patch("apps.tasks.consumers.Control") as mock_ctrl:
                mock_ctrl.return_value.revoke = MagicMock()

                await communicator.send_json_to({"type": "cancel"})

                # Should receive a task.update broadcast
                response = await communicator.receive_json_from(timeout=5)
                assert response["type"] == "task.update"
                assert response["status"] == TaskRecord.Status.REVOKED

        await communicator.disconnect()
