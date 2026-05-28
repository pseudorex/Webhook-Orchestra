from fastapi import APIRouter, Depends, HTTPException
from app.api.dependencies.database import get_db
from app.api.dependencies.auth import get_current_tenant
from app.models.tenant import Tenant
from app.models.dead_letter_event import DeadLetterEvent
from app.models.event import Event

router = APIRouter(
    prefix="/dlq",
    tags=["DLQ"]
)

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from worker.tasks import deliver_webhook

@router.get("/")
async def get_dead_events(
    skip: int = 0,
    limit: int = 10,
    db: AsyncSession = Depends(get_db),
    tenant: Tenant = Depends(get_current_tenant)
):
    result = await db.execute(
        select(DeadLetterEvent)
        .join(Event, DeadLetterEvent.original_event_id == Event.id)
        .where(Event.tenant_id == tenant.id)
        .offset(skip)
        .limit(limit)
    )
    events = result.scalars().all()
    return events


@router.post("/{dead_event_id}/replay")
async def replay_dead_event(
    dead_event_id: int,
    db: AsyncSession = Depends(get_db),
    tenant: Tenant = Depends(get_current_tenant)
):
    # Fetch dead letter event joined with Event to verify ownership
    result = await db.execute(
        select(DeadLetterEvent)
        .join(Event, DeadLetterEvent.original_event_id == Event.id)
        .where(
            DeadLetterEvent.id == dead_event_id,
            Event.tenant_id == tenant.id
        )
    )
    dead_event = result.scalar_one_or_none()

    if not dead_event:
        raise HTTPException(
            status_code=404,
            detail="Dead event not found"
        )

    result = await db.execute(
        select(Event).where(
            Event.id == dead_event.original_event_id
        )
    )
    event = result.scalar_one_or_none()

    if not event:
        raise HTTPException(
            status_code=404,
            detail="Original event not found"
        )

    event.status = "retrying"
    event.retry_count = 0
    event.failure_type = None
    event.last_error = None

    dead_event.replay_count += 1
    await db.commit()

    deliver_webhook.apply_async(
        args=[event.id],
        queue="low_priority"
    )

    return {"message": "Replay scheduled"}