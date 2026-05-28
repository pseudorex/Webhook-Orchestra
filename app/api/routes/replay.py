from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.event import Event
from app.models.tenant import Tenant
from app.api.dependencies.auth import get_current_tenant

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
        tenant: Tenant = Depends(get_current_tenant)
):
    # Retrieve and verify event ownership first
    event = await db.get(Event, event_id)
    if not event or event.tenant_id != tenant.id:
        raise HTTPException(
            status_code=404,
            detail="Event not found"
        )

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