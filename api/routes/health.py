"""Is everything the API depends on reachable?"""

import asyncio

from fastapi import APIRouter, Request, Response

router = APIRouter(tags=["health"])


@router.get("/health")
async def health(request: Request, response: Response):
    """PostgreSQL, TimescaleDB and Redis. 200 when all are up, 503 otherwise."""
    checks = {}

    try:
        async with asyncio.timeout(3):
            async with request.app.state.pg.connection() as conn:
                row = await (await conn.execute(
                    "SELECT current_database() AS database,"
                    " (SELECT extversion FROM pg_extension WHERE extname = 'timescaledb')"
                    " AS timescaledb")).fetchone()
        checks["postgres"] = {"ok": True, "database": row["database"]}
        checks["timescaledb"] = {"ok": row["timescaledb"] is not None,
                                 "version": row["timescaledb"]}
    except Exception as exc:
        checks["postgres"] = {"ok": False, "error": type(exc).__name__}
        checks["timescaledb"] = {"ok": False}

    try:
        async with asyncio.timeout(2):
            await request.app.state.redis.ping()
        checks["redis"] = {"ok": True}
    except Exception as exc:
        checks["redis"] = {"ok": False, "error": type(exc).__name__}

    ok = all(c["ok"] for c in checks.values())
    response.status_code = 200 if ok else 503
    return {"status": "ok" if ok else "degraded", "checks": checks}
