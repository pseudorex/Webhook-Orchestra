from fastapi import APIRouter, Request, Header, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.models.processed_webhook import ProcessedWebhook
from app.api.dependencies.database import get_db           # ← use the async dep
from app.services.signature_service import generate_signature

router = APIRouter()


@router.post("/webhook")
async def receive_webhook(
    request: Request,
    x_event_id: str = Header(...),
    x_webhook_signature: str = Header(None),                # ← accept signature
    db: AsyncSession = Depends(get_db)                      # ← inject async session
):
    payload = await request.json()

    # CHECK DUPLICATE
    result = await db.execute(
        select(ProcessedWebhook).where(
            ProcessedWebhook.event_id == x_event_id
        )
    )
    existing_event = result.scalar_one_or_none()

    if existing_event:
        return {"message": "Duplicate ignored"}

    # STORE PROCESSED EVENT
    processed_event = ProcessedWebhook(event_id=x_event_id)
    db.add(processed_event)
    await db.commit()

    return {"message": "Webhook processed successfully"}