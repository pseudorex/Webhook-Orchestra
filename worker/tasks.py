from worker.celery_app import celery
from worker.database import SessionLocal

from app.models.event import Event

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
    event_id,
    endpoint_url=None,
    subscription_id=None
):
    start_time = time.time()
    status = "success"
    db = SessionLocal()
    try:
        event = db.get(Event, event_id)
        if not event:
            logger.error(f"Event not found in database", extra={"event_id": event_id})
            status = "failure"
            return
        tenant_id_var.set(event.tenant_id)
        event_id_var.set(event.id)
        WebhookEngine.process_event(
            db=db,
            event=event,
            endpoint_url=endpoint_url,
            subscription_id=subscription_id
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
