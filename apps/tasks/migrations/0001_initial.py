# Generated migration for TaskRecord model — Phase 3

import django.db.models.deletion
import django.utils.timezone
import uuid
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="TaskRecord",
            fields=[
                (
                    "id",
                    models.UUIDField(
                        default=uuid.uuid4,
                        editable=False,
                        primary_key=True,
                        serialize=False,
                    ),
                ),
                ("name", models.CharField(max_length=200)),
                (
                    "task_type",
                    models.CharField(
                        choices=[
                            ("file_processing", "File Processing"),
                            ("data_export", "Data Export"),
                            ("email_batch", "Email Batch"),
                        ],
                        max_length=30,
                    ),
                ),
                (
                    "priority",
                    models.IntegerField(
                        choices=[(1, "Low"), (5, "Default"), (10, "High")],
                        default=5,
                    ),
                ),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("pending",  "Pending"),
                            ("sent",     "Sent"),
                            ("started",  "Started"),
                            ("progress", "In Progress"),
                            ("success",  "Success"),
                            ("failure",  "Failure"),
                            ("retry",    "Retry"),
                            ("revoked",  "Revoked"),
                        ],
                        db_index=True,
                        default="pending",
                        max_length=20,
                    ),
                ),
                ("celery_task_id", models.CharField(blank=True, max_length=255)),
                ("progress", models.IntegerField(default=0)),
                ("progress_message", models.CharField(blank=True, max_length=500)),
                ("payload", models.JSONField(default=dict)),
                ("result",  models.JSONField(blank=True, null=True)),
                ("error_message", models.TextField(blank=True)),
                ("retry_count",   models.IntegerField(default=0)),
                ("max_retries",   models.IntegerField(default=3)),
                (
                    "created_at",
                    models.DateTimeField(auto_now_add=True),
                ),
                (
                    "updated_at",
                    models.DateTimeField(auto_now=True),
                ),
                ("started_at",   models.DateTimeField(blank=True, null=True)),
                ("completed_at", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "verbose_name": "Task Record",
                "verbose_name_plural": "Task Records",
                "ordering": ["-created_at"],
            },
        ),
        migrations.AddIndex(
            model_name="taskrecord",
            index=models.Index(fields=["status"],    name="taskrecord_status_idx"),
        ),
        migrations.AddIndex(
            model_name="taskrecord",
            index=models.Index(fields=["task_type"], name="taskrecord_task_type_idx"),
        ),
        migrations.AddIndex(
            model_name="taskrecord",
            index=models.Index(fields=["priority"],  name="taskrecord_priority_idx"),
        ),
        migrations.AddIndex(
            model_name="taskrecord",
            index=models.Index(fields=["created_at"], name="taskrecord_created_at_idx"),
        ),
    ]
