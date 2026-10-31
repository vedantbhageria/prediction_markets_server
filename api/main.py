"""The FastAPI application.

    uvicorn api.main:app --host 127.0.0.1 --port 8000

Run one process (no --workers): the backfill worker lives inside it.
Interactive docs are served at /docs.
"""

import logging
from contextlib import asynccontextmanager

import psycopg2
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException

import db
import ratelimit
import store
from api import __version__, config, jobs
from api.common import ApiError, error_response
from api.routes import health, v1

log = logging.getLogger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(message)s", datefmt="%H:%M:%S")
    ratelimit.DEFAULT = "warm"
    db.get_pool(1, config.DB_POOL)
    try:
        store.ensure_schema()
    except Exception as exc:
        # The database may be starting too; /health says so until it is up.
        log.warning("could not check the schema: %s: %s", type(exc).__name__, exc)
    worker = jobs.Worker(config.JOB_WORKERS)
    worker.start()
    try:
        yield
    finally:
        worker.shutdown()
        db.close_pool()


app = FastAPI(title="prediction_markets API", version=__version__, lifespan=lifespan,
              description="Kalshi markets and one-minute trade candles. Send your key in the"
                          " X-API-Key header. Lists take format=json, csv or parquet.")
app.include_router(health.router)
app.include_router(v1.router)


@app.middleware("http")
async def rate_headers(request: Request, call_next):
    response = await call_next(request)
    limits = getattr(request.state, "ratelimit", None)
    if limits:
        response.headers["X-RateLimit-Limit"] = str(limits[0])
        response.headers["X-RateLimit-Remaining"] = str(limits[1])
        response.headers["X-RateLimit-Reset"] = str(limits[2])
    return response


@app.exception_handler(ApiError)
async def api_error(request: Request, exc: ApiError):
    return error_response(exc.status, exc.code, exc.message, exc.headers)


@app.exception_handler(HTTPException)
async def http_error(request: Request, exc: HTTPException):
    codes = {404: "not_found", 405: "method_not_allowed"}
    return error_response(exc.status_code, codes.get(exc.status_code, "http_error"),
                          str(exc.detail))


@app.exception_handler(RequestValidationError)
async def bad_parameter(request: Request, exc: RequestValidationError):
    first = exc.errors()[0]
    where = ".".join(str(p) for p in first["loc"][1:]) or "request"
    return error_response(400, "invalid_parameter", f"{where}: {first['msg']}")


@app.exception_handler(psycopg2.errors.QueryCanceled)
async def too_slow(request: Request, exc: Exception):
    return error_response(503, "query_timeout", f"the query took more than"
                          f" {config.STATEMENT_TIMEOUT}s; ask for less at a time")


@app.exception_handler(psycopg2.OperationalError)
async def database_down(request: Request, exc: Exception):
    return error_response(503, "database_unavailable", "the database is not answering;"
                          " try again shortly", {"Retry-After": "10"})


@app.exception_handler(Exception)
async def internal(request: Request, exc: Exception):
    log.exception("%s %s failed", request.method, request.url.path)
    return error_response(500, "internal", f"{type(exc).__name__}; see the API's log")


@app.get("/", tags=["meta"], include_in_schema=False)
def root():
    return {"name": "prediction_markets API", "version": __version__,
            "docs": "/docs", "health": "/health", "v1": "/v1"}
