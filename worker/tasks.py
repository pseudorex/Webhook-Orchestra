import requests

from datetime import datetime

from worker.celery_app import celery
from worker.database import SessionLocal

from app.models.event import Event
from app.models.tenant import Tenant

# NEW IMPORT
from app.services.signature_service import generate_signature


@celery.task(
    bind=True,
    max_retries=3
)
def deliver_webhook(
    self,
    event_id
):

    db = SessionLocal()
    event = None

    try:

        # FETCH EVENT
        event = db.get(Event, event_id)

        if not event:

            print("EVENT NOT FOUND")
            return

        # FETCH TENANT
        tenant = db.get(Tenant, event.tenant_id)

        if not tenant:

            print("TENANT NOT FOUND")
            return

        webhook_url = tenant.webhook_url

        # NEW CHECK
        if not webhook_url:

            print("WEBHOOK URL NOT CONFIGURED")
            return

        # PAYLOAD
        payload = {
            "event_id": event.id,
            "event_type": event.event_type,
            "payload": event.payload
        }

        # NEW SIGNATURE GENERATION
        signature = generate_signature(
            payload=payload,
            secret=tenant.webhook_secret
        )

        # SEND WEBHOOK
        response = requests.post(
            webhook_url,
            json=payload,

            # NEW HEADERS
            headers={
                "X-Webhook-Signature": signature
            },

            timeout=10
        )

        response.raise_for_status()

        # SUCCESS STATE
        event.status = "delivered"

        event.delivered_at = datetime.utcnow()

        event.last_error = None

        db.commit()

        print("=================================")
        print("WEBHOOK DELIVERED")
        print("STATUS:", response.status_code)
        print("=================================")

    except Exception as e:

        # FIXED SAFE ERROR HANDLING
        if event:
            event.retry_count = self.request.retries + 1
            event.last_error = str(e)

            # DEAD STATE
            if self.request.retries >= self.max_retries:

                event.status = "dead"

                db.commit()

                print("=================================")
                print("MAX RETRIES EXCEEDED")
                print("EVENT MOVED TO DEAD STATE")
                print("=================================")

                return

            # RETRYING STATE
            event.status = "retrying"

            db.commit()

        countdown = 2 ** self.request.retries

        raise self.retry(
            exc=e,
            countdown=countdown
        )

    finally:

        db.close()