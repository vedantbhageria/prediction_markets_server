"""The FastAPI application.

    uvicorn api.main:app --host 127.0.0.1 --port 8000

Interactive docs are served at /docs once it is running.
"""

from fastapi import FastAPI

from api import __version__
from api.deps import lifespan
from api.routes import health, v1

app = FastAPI(title="prediction_markets API", version=__version__, lifespan=lifespan)
app.include_router(health.router)
app.include_router(v1.router)


@app.get("/", tags=["meta"])
async def root():
    return {"name": "prediction_markets API", "version": __version__,
            "docs": "/docs", "health": "/health"}
