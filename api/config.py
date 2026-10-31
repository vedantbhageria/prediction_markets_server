"""API settings, read from the environment / the project's .env.

The database settings are the same PG* variables the collector uses, so the
API and the collector always point at the same database.
"""

import os

from db import load_env

load_env()

API_HOST = os.environ.get("API_HOST", "127.0.0.1")
API_PORT = int(os.environ.get("API_PORT", "8000"))

PG_DSN = (f"host={os.environ.get('PGHOST', 'localhost')}"
          f" port={os.environ.get('PGPORT', '5432')}"
          f" dbname={os.environ.get('PGDATABASE', 'prediction_markets')}"
          f" user={os.environ.get('PGUSER', 'postgres')}"
          f" password={os.environ.get('PGPASSWORD', '')}")
PG_POOL_MIN = int(os.environ.get("API_PG_POOL_MIN", "1"))
PG_POOL_MAX = int(os.environ.get("API_PG_POOL_MAX", "10"))

REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")
