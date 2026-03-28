"""
DRF serializers for the tasks app.
"""

from rest_framework import serializers

from .models import TaskRecord


class TaskRecordSerializer(serializers.ModelSerializer):
    """
    Full read-only serializer for TaskRecord — used for list/retrieve.
    duration_seconds is computed from the model method.
    """

    duration_seconds = serializers.SerializerMethodField()

    class Meta:
        model = TaskRecord
        fields = [
            "id",
            "name",
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
            "duration_seconds",
        ]
        read_only_fields = fields  # every field is read-only on this serializer

    def get_duration_seconds(self, obj) -> float | None:
        return obj.duration_seconds()


class TaskSubmitSerializer(serializers.Serializer):
    """
    Write serializer for submitting a new task.
    Only the fields the client needs to provide are included here.
    """

    VALID_TASK_TYPES = [choice[0] for choice in TaskRecord.TaskType.choices]
    VALID_PRIORITIES = [choice[0] for choice in TaskRecord.Priority.choices]

    name = serializers.CharField(max_length=200)
    task_type = serializers.CharField(max_length=30)
    priority = serializers.IntegerField(default=TaskRecord.Priority.DEFAULT)
    payload = serializers.JSONField()

    def validate_task_type(self, value: str) -> str:
        if value not in self.VALID_TASK_TYPES:
            raise serializers.ValidationError(
                f"Invalid task_type '{value}'. Must be one of: {self.VALID_TASK_TYPES}."
            )
        return value

    def validate_priority(self, value: int) -> int:
        if value not in self.VALID_PRIORITIES:
            raise serializers.ValidationError(
                f"Invalid priority '{value}'. Must be one of: {self.VALID_PRIORITIES}."
            )
        return value

    def validate_payload(self, value) -> dict:
        if not isinstance(value, dict) or not value:
            raise serializers.ValidationError(
                "payload must be a non-empty JSON object."
            )
        return value
