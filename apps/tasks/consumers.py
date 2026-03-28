"""
WebSocket consumer for real-time task progress updates.

Clients connect to:  ws://host/ws/tasks/<task_id>/

On connect  → validates UUID + TaskRecord existence, joins group, sends current state.
On receive  → handles "ping" and "cancel" commands.
On event    → forwards task.progress / task.update events to the client.
"""

import json
import logging
from uuid import UUID

from channels.generic.websocket import AsyncJsonWebsocketConsumer
from django.utils import timezone

logger = logging.getLogger(__name__)


class TaskProgressConsumer(AsyncJsonWebsocketConsumer):
    """
    Async JSON WebSocket consumer that streams live task progress to clients.
    One consumer instance exists per connected client.
    """

    # -----------------------------------------------------------------------
    # Connection lifecycle
    # -----------------------------------------------------------------------
    async def connect(self):
        task_id_str = self.scope["url_route"]["kwargs"].get("task_id", "")

        # 1. Validate UUID format
        try:
            self.task_id = str(UUID(task_id_str))
        except (ValueError, AttributeError):
            logger.warning("WebSocket rejected: invalid UUID '%s'.", task_id_str)
            await self.close(code=4004)
            return

        # 2. Validate TaskRecord exists (sync ORM → run in thread pool)
        from asgiref.sync import sync_to_async
        from apps.tasks.models import TaskRecord

        try:
            record = await sync_to_async(TaskRecord.objects.get)(pk=self.task_id)
        except TaskRecord.DoesNotExist:
            logger.warning("WebSocket rejected: TaskRecord %s not found.", self.task_id)
            await self.close(code=4004)
            return

        # 3. Join the task's channel group
        self.group_name = f"task_{self.task_id}"
        await self.channel_layer.group_add(self.group_name, self.channel_name)

        # 4. Accept the connection
        await self.accept()

        # 5. Send the current task state immediately so the client is in sync
        await self.send_json(
            {
                "type": "task.state",
                "task_id": self.task_id,
                "status": record.status,
                "progress": record.progress,
                "message": record.progress_message,
                "name": record.name,
                "task_type": record.task_type,
            }
        )
        logger.info("WebSocket connected for task %s.", self.task_id)

    async def disconnect(self, close_code):
        if hasattr(self, "group_name"):
            await self.channel_layer.group_discard(self.group_name, self.channel_name)
            logger.info(
                "WebSocket disconnected for task %s (code=%s).",
                getattr(self, "task_id", "?"),
                close_code,
            )

    # -----------------------------------------------------------------------
    # Receive from client
    # -----------------------------------------------------------------------
    async def receive_json(self, content, **kwargs):
        msg_type = content.get("type") or content.get("action")

        if msg_type == "ping":
            await self.send_json({"type": "pong"})
            return

        if msg_type == "cancel":
            await self._handle_cancel()
            return

        logger.debug("Unhandled WebSocket message type '%s'.", msg_type)

    async def _handle_cancel(self):
        """Revoke the Celery task and update TaskRecord status."""
        from asgiref.sync import sync_to_async
        from celery.app.control import Control
        from config.celery import app as celery_app
        from apps.tasks.models import TaskRecord

        try:
            record = await sync_to_async(TaskRecord.objects.get)(pk=self.task_id)
        except TaskRecord.DoesNotExist:
            await self.send_json({"type": "error", "message": "Task not found."})
            return

        if record.celery_task_id:
            control = Control(celery_app)
            await sync_to_async(control.revoke)(
                record.celery_task_id, terminate=True, signal="SIGTERM"
            )

        await sync_to_async(
            lambda: TaskRecord.objects.filter(pk=self.task_id).update(
                status=TaskRecord.Status.REVOKED
            )
        )()

        # Notify all connected clients in the group
        await self.channel_layer.group_send(
            self.group_name,
            {
                "type": "task.update",
                "task_id": self.task_id,
                "status": TaskRecord.Status.REVOKED,
                "progress": record.progress,
                "message": "Task cancelled by client.",
            },
        )
        logger.info("Task %s cancelled via WebSocket.", self.task_id)

    # -----------------------------------------------------------------------
    # Group message handlers
    # (method names use underscores; Channels maps "task.progress" → task_progress)
    # -----------------------------------------------------------------------
    async def task_progress(self, event):
        """Forward a progress update from the channel group to this client."""
        await self.send_json(
            {
                "type": "task.progress",
                "task_id": event.get("task_id"),
                "status": event.get("status"),
                "progress": event.get("progress"),
                "message": event.get("message"),
            }
        )

    async def task_update(self, event):
        """Forward a full status update from the channel group to this client."""
        await self.send_json(
            {
                "type": "task.update",
                "task_id": event.get("task_id"),
                "status": event.get("status"),
                "progress": event.get("progress"),
                "message": event.get("message"),
            }
        )
