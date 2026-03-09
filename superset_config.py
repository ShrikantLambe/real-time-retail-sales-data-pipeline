"""
Superset configuration for the retail pipeline Docker environment.

This file is mounted into the Superset container at /app/superset_config.py
(via SUPERSET_CONFIG_PATH env var). It overrides the default SQLite metadata
database with a Postgres instance shared with Airflow.
"""
import os

# ── Metadata database ─────────────────────────────────────────────────────────
# Use the shared Postgres instance so user accounts and dashboard state survive
# container restarts. Falls back to the default SQLite if not set.
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SQLALCHEMY_DATABASE_URI",
    "sqlite:////app/superset_home/superset.db",
)

# ── Security ──────────────────────────────────────────────────────────────────
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# ── Feature flags ─────────────────────────────────────────────────────────────
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# ── Session cookie ────────────────────────────────────────────────────────────
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SECURE = False   # Set True behind HTTPS in production
