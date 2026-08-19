# =============================================================================
# Apache Superset configuration (local development)
#
# Mounted at /app/superset_config/config.py and selected via
# SUPERSET_CONFIG_PATH. Connection details come from environment variables
# set in docker/docker-compose.yml (which fall back to repo .env values).
# =============================================================================
import os

# --- Metadata database -------------------------------------------------------
# Dedicated `superset` database inside the shared local Postgres container
# (the same Postgres backs the Hive Metastore with its `hive` database).
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SUPERSET_DB_URI",
    "postgresql://hive:hive@postgres:5432/superset",
)
SQLALCHEMY_ECHO = False
SQLALCHEMY_TRACK_MODIFICATIONS = False

# --- Security -----------------------------------------------------------------
# Local development only - do not use this secret outside this laptop.
SUPERSET_SECRET_KEY = os.environ.get(
    "SUPERSET_SECRET_KEY", "local-dev-superset-secret"
)

# No self-service registration; the bootstrap script creates the admin user.
SUPERSET_USER_REGISTRATION = False
SUPERSET_USER_REGISTRATION_ROLE = "Public"

# --- Behavior -------------------------------------------------------------------
# We ship our own dashboards (gold-layer marts); skip the examples database.
LOAD_EXAMPLES = False

# Dashboard timezone (data is synthetic 2024, UTC keeps it deterministic).
TZ = "UTC"

# Keep Superset from trying to email anyone.
SMTP_MAIL_FROM = "superset@localhost"
