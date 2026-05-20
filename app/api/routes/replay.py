from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies.database import get_db
from app.services.replay_service import (
    replay_event,
    ReplayError,
)

router = APIRouter(
    prefix="/events",
    tags=["Replay"],
)


@router.post("/{event_id}/replay")
async def replay_event_endpoint(
    event_id: int,
    db: AsyncSession = Depends(get_db),
):
    try:
        result = await replay_event(
            event_id=event_id,
            db=db,
        )

        return result

    except ReplayError as e:
        raise HTTPException(
            status_code=400,
            detail=str(e),
        )