"""Version 1 of the data API. Endpoints are added here as they are built."""

from fastapi import APIRouter

router = APIRouter(prefix="/v1", tags=["v1"])


@router.get("")
async def index():
    """What this version offers so far."""
    return {"version": 1, "endpoints": []}
