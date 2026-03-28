"""
config/__init__.py

Import the Celery app here so it is initialised whenever Django starts.
This ensures that shared_task decorators in all apps resolve to this instance.
"""

from .celery import app as celery_app  # noqa: F401

__all__ = ("celery_app",)
