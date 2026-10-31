"""Shared connections: one PostgreSQL pool and one Redis client per process,
opened when the app starts and closed when it stops."""

from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI, Request
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from api import config


@asynccontextmanager
async def lifespan(app: FastAPI):
    # open=False then open(wait=False): the API starts even if the database is
    # briefly down, and /health reports it rather than the process crashing.
    pool = AsyncConnectionPool(config.PG_DSN, min_size=config.PG_POOL_MIN,
                               max_size=config.PG_POOL_MAX, open=False,
                               kwargs={"row_factory": dict_row})
    await pool.open(wait=False)
    app.state.pg = pool
    app.state.redis = aioredis.from_url(config.REDIS_URL, decode_responses=True)
    try:
        yield
    finally:
        await app.state.redis.aclose()
        await pool.close()


async def db(request: Request):
    """A pooled database connection for the length of one request."""
    async with request.app.state.pg.connection() as conn:
        yield conn


def cache(request: Request):
    return request.app.state.redis
