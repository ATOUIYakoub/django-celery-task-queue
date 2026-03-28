"""
Development settings — extends base with debug tooling.
"""

from .base import *  # noqa: F401, F403

DEBUG = True

INSTALLED_APPS += [  # noqa: F405
    # Add dev-only apps here if needed (e.g. django-debug-toolbar)
]

# Looser security for local development
CORS_ALLOW_ALL_ORIGINS = True
