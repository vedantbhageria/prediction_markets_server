"""Is everything the API depends on reachable? The one endpoint without a key."""

from fastapi import APIRouter

import db
import ratelimit
from api.common import json_response

router = APIRouter(tags=["meta"])


@router.get("/health", summary="PostgreSQL, TimescaleDB and Redis: 200 if all are up, else 503")
def health():
    checks = {}
    try:
        with db.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = 3000")
            cur.execute("SELECT (SELECT extversion FROM pg_extension"
                        " WHERE extname = 'timescaledb') AS timescaledb")
            version = cur.fetchone()["timescaledb"]
        checks["postgres"] = {"ok": True}
        checks["timescaledb"] = {"ok": version is not None, "version": version}
    except Exception as exc:
        checks["postgres"] = {"ok": False, "error": type(exc).__name__}
        checks["timescaledb"] = {"ok": False}
    try:
        ratelimit.client().ping()
        checks["redis"] = {"ok": True}
    except Exception as exc:
        checks["redis"] = {"ok": False, "error": type(exc).__name__}
    ok = all(c["ok"] for c in checks.values())
    return json_response({"status": "ok" if ok else "degraded", "checks": checks},
                         200 if ok else 503)
