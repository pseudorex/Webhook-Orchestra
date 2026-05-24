from fastapi import (
    APIRouter,
    Depends, HTTPException
)
from app.api.dependencies.database import get_db
from app.models.dead_letter_event import (
    DeadLetterEvent
)
from app.models.event import Event

router = APIRouter(
    prefix="/dlq",
    tags=["DLQ"]
)


from sqlalchemy.ext.asyncio import AsyncSession    # ← replace sqlalchemy.orm.Session
from sqlalchemy import select                       # ← add this import
from worker.tasks import deliver_webhook            # ← add this to actually enqueue

@router.get("/")
async def get_dead_events(                          # ← add async
    skip: int = 0,
    limit: int = 10,
    db: AsyncSession = Depends(get_db)              # ← AsyncSession
):
    result = await db.execute(                      # ← await db.execute + select()
        select(DeadLetterEvent)
        .offset(skip)
        .limit(limit)
    )
    events = result.scalars().all()
    return events


@router.post("/{dead_event_id}/replay")
async def replay_dead_event(                        # ← add async
    dead_event_id: int,
    db: AsyncSession = Depends(get_db)              # ← AsyncSession
):
    result = await db.execute(                      # ← await + select
        select(DeadLetterEvent).where(
            DeadLetterEvent.id == dead_event_id
        )
    )
    dead_event = result.scalar_one_or_none()

    if not dead_event:
        raise HTTPException(                        # ← use HTTPException, not dict
            status_code=404,
            detail="Dead event not found"
        )

    result = await db.execute(                      # ← await + select
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

    event.status = "retrying"                       # ← lowercase, matching webhook_engine
    event.retry_count = 0
    event.failure_type = None
    event.last_error = None

    dead_event.replay_count += 1

    await db.commit()                               # ← single await commit

    deliver_webhook.apply_async(
        args=[event.id],
        queue="low_priority"
    )

    return {"message": "Replay scheduled"}