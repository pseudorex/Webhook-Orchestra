from fastapi import APIRouter, Request, Header
from sqlalchemy.orm import Session

from app.models.processed_webhook import ProcessedWebhook
from worker.database import SessionLocal

router = APIRouter()


@router.post("/webhook")
async def receive_webhook(
    request: Request,
    x_event_id: str = Header(...)
):

    db: Session = SessionLocal()

    try:

        # READ PAYLOAD FIRST
        payload = await request.json()

        # CHECK DUPLICATE
        existing_event = db.query(
            ProcessedWebhook
        ).filter(
            ProcessedWebhook.event_id == x_event_id
        ).first()

        # DUPLICATE DETECTED
        if existing_event:

            return {
                "message": "Duplicate ignored"
            }

        print("PROCESSING WEBHOOK")
        print(payload)

        # STORE PROCESSED EVENT
        processed_event = ProcessedWebhook(
            event_id=x_event_id
        )

        db.add(processed_event)

        db.commit()

        return {
            "message": "Webhook processed successfully"
        }

    finally:

        db.close()