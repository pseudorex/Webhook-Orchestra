from celery import Celery
from kombu import Queue
from celery.signals import worker_ready
import os
import logging
from prometheus_client import start_http_server
import app.core.logging

logger = logging.getLogger(__name__)

celery = Celery(
    "webhook_tasks",
    broker="redis://redis:6379/0",
    backend="redis://redis:6379/0",
    include=["worker.tasks"]
)

celery.conf.task_queues = (
    Queue("high_priority", routing_key="high_priority"),
    Queue("default", routing_key="default"),
    Queue("low_priority", routing_key="low_priority"),
)

@worker_ready.connect
def start_metrics_server(**kwargs):
    port = int(os.getenv("METRICS_PORT", 8001))
    try:
        # Start in-memory Prometheus metrics server for this container process
        start_http_server(port)
        logger.info(f"Prometheus worker metrics server started on port {port}")
    except Exception as e:
        logger.error(f"Failed to start Prometheus worker metrics server: {e}", exc_info=True)