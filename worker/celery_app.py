from celery import Celery
from kombu import Queue
# Importing this file registers Celery signals (setup_logging, prerun, postrun, before_task_publish)
import app.core.logging

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