from worker.celery_app import celery
from worker.database import SessionLocal

from app.models.event import Event

from app.services.reliability.webhook_engine import (
    WebhookEngine
)


@celery.task(bind=True)
def deliver_webhook(
    self,
    event_id,
    endpoint_url=None,       # <-- Changed: Made optional (default to None)
    subscription_id=None
):

    db = SessionLocal()

    try:

        event = db.get(Event, event_id)

        if not event:

            print("EVENT NOT FOUND")

            return

        WebhookEngine.process_event(
            db=db,
            event=event,
            endpoint_url=endpoint_url,
            subscription_id=subscription_id
        )

    finally:

        db.close()