from worker.celery_app import celery
from worker.database import SessionLocal

from app.models.event import Event
from app.models.subscription_delivery import SubscriptionDelivery # ← Added import for SubscriptionDelivery
from app.models.subscription import Subscription # ← Added import for Subscription

from app.services.reliability.webhook_engine import (
    WebhookEngine
)
import logging
from app.core.logging import tenant_id_var, event_id_var

logger = logging.getLogger(__name__)
import time
from app.core.metrics import WORKER_TASKS_PROCESSED_TOTAL, WORKER_TASK_DURATION

@celery.task(bind=True)
def deliver_webhook(
    self,
    subscription_delivery_id # ← Changed argument to subscription_delivery_id
):
    start_time = time.time()
    status = "success"
    db = SessionLocal()
    try:
        # Load the subscription delivery context
        delivery = db.get(SubscriptionDelivery, subscription_delivery_id)
        if not delivery:
            logger.error(f"SubscriptionDelivery not found in database", extra={"delivery_id": subscription_delivery_id})
            status = "failure"
            return

        event = db.get(Event, delivery.event_id)
        if not event:
            logger.error(f"Event not found in database", extra={"event_id": delivery.event_id})
            status = "failure"
            return

        tenant_id_var.set(event.tenant_id)
        event_id_var.set(event.id)
        
        # Process delivery on the SubscriptionDelivery record
        WebhookEngine.process_event(
            db=db,
            delivery=delivery
        )
    except Exception as e:
        status = "failure"
        raise e
    finally:
        db.close()
        duration = time.time() - start_time
        # Record Celery task execution statistics
        WORKER_TASKS_PROCESSED_TOTAL.labels(task_name=self.name, status=status).inc()
        WORKER_TASK_DURATION.labels(task_name=self.name).observe(duration)
