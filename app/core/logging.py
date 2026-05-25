import logging
import json
import os
import sys
import uuid
import contextvars
from datetime import datetime, timezone
from celery.signals import setup_logging, before_task_publish, task_prerun, task_postrun

# Context variables for thread/task-local storage
correlation_id_var = contextvars.ContextVar("correlation_id", default=None)
tenant_id_var = contextvars.ContextVar("tenant_id", default=None)
event_id_var = contextvars.ContextVar("event_id", default=None)


class StructuredJsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Inject context variables if set
        corr_id = correlation_id_var.get()
        if corr_id:
            log_data["correlation_id"] = corr_id

        t_id = tenant_id_var.get()
        if t_id:
            log_data["tenant_id"] = t_id

        e_id = event_id_var.get()
        if e_id:
            log_data["event_id"] = e_id

        # Also support passing extra keys directly in logger calls
        for key in ["correlation_id", "tenant_id", "event_id"]:
            val = getattr(record, key, None)
            if val:
                log_data[key] = val

        # Handle tracebacks for exceptions
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data)


def setup_app_logging():
    """Configures the root logger with the StructuredJsonFormatter."""
    log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_name, logging.INFO)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Remove existing handlers to avoid duplicates
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(StructuredJsonFormatter())
    root_logger.addHandler(handler)

    # Redirect uvicorn loggers to use the root config
    for name in ["uvicorn", "uvicorn.access", "uvicorn.error"]:
        ul = logging.getLogger(name)
        ul.handlers = []
        ul.propagate = True

    return root_logger


# ==========================================
# CELERY SIGNALS FOR PROPAGATING CONTEXT
# ==========================================

@setup_logging.connect
def config_celery_loggers(*args, **kwargs):
    """Overrides Celery's default logging configuration to use JSON format."""
    setup_app_logging()


@before_task_publish.connect
def before_task_publish_handler(headers=None, **kwargs):
    """Propagates context vars from request thread to Celery message headers."""
    if headers is not None:
        headers["correlation_id"] = correlation_id_var.get()
        headers["tenant_id"] = tenant_id_var.get()
        headers["event_id"] = event_id_var.get()


@task_prerun.connect
def task_prerun_handler(task=None, args=None, **kwargs):
    """Restores context variables when a Celery worker starts a task."""
    request = task.request if task else None
    if request:
        # 1. Try reading from request attributes (Celery flattens custom headers here)
        corr_id = getattr(request, "correlation_id", None)
        t_id = getattr(request, "tenant_id", None)
        e_id = getattr(request, "event_id", None)

        # 2. Fallback to request.headers dictionary if attributes are missing
        headers = getattr(request, "headers", {}) or {}
        if not corr_id:
            corr_id = headers.get("correlation_id")
        if not t_id:
            t_id = headers.get("tenant_id")
        if not e_id:
            e_id = headers.get("event_id")

        # 3. Apply to logging context vars (or fallback to generating a new UUID for correlation ID)
        if corr_id:
            correlation_id_var.set(corr_id)
        else:
            correlation_id_var.set(str(uuid.uuid4()))

        if t_id:
            tenant_id_var.set(t_id)

        if e_id:
            event_id_var.set(e_id)
        elif task.name == "worker.tasks.deliver_webhook" and args:
            event_id_var.set(args[0])

@task_postrun.connect
def task_postrun_handler(**kwargs):
    """Clears context variables after a Celery task finishes."""
    correlation_id_var.set(None)
    tenant_id_var.set(None)
    event_id_var.set(None)