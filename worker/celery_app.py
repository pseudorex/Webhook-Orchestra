from celery import Celery
from kombu import Queue
from celery.signals import worker_ready
import os
import logging
from prometheus_client import start_http_server
from app.core.tracing import setup_tracing, instrument_celery
setup_tracing("webhook-worker")
instrument_celery()


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


# Change the start_metrics_server function in worker/celery_app.py to look like this:

@worker_ready.connect
def start_metrics_server(**kwargs):
    port = int(os.getenv("METRICS_PORT", 8001))
    try:
        # 1. Import our custom metrics registry
        from app.core.metrics import REGISTRY

        # 2. Pass the registry so the HTTP server exposes the custom metrics
        start_http_server(port, registry=REGISTRY)

        logger.info(f"Prometheus worker metrics server started on port {port} using custom registry")
    except Exception as e:
        logger.error(f"Failed to start Prometheus worker metrics server: {e}", exc_info=True)