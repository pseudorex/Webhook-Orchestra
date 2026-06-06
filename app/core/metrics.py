from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry
import os
import httpx

# Create custom registry to prevent mix-ups with python system metrics
REGISTRY = CollectorRegistry()

# 1. Delivery Failure & Latency Metrics
WEBHOOK_DELIVERY_ATTEMPTS_TOTAL = Counter(
    "webhook_delivery_attempts_total",
    "Total number of webhook delivery attempts",
    ["tenant_id", "status_code", "error_class"],
    registry=REGISTRY
)

WEBHOOK_DELIVERY_LATENCIES_SECONDS = Histogram(
    "webhook_delivery_latencies_seconds",
    "Webhook delivery latency in seconds",
    ["tenant_id"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
    registry=REGISTRY
)

# 2. Retry Metrics
WEBHOOK_RETRIES_TOTAL = Counter(
    "webhook_retries_total",
    "Total number of webhook retry attempts scheduled",
    ["tenant_id", "retry_count"],
    registry=REGISTRY
)

# 3. DLQ Metrics
WEBHOOK_DLQ_MOVES_TOTAL = Counter(
    "webhook_dlq_moves_total",
    "Total number of events moved to the DLQ",
    ["tenant_id", "failure_type"],
    registry=REGISTRY
)

# 4. Queue Length Gauge
WEBHOOK_QUEUE_LENGTH = Gauge(
    "webhook_queue_length",
    "Current length of the Celery queue in RabbitMQ",
    ["queue_name"],
    registry=REGISTRY
)

# 5. Alerting System Gauges (Fetched dynamically)
WEBHOOK_DLQ_SIZE = Gauge(
    "webhook_dlq_size",
    "Current size of the Dead Letter Queue",
    registry=REGISTRY
)

WEBHOOK_ENDPOINT_HEALTH_SCORE = Gauge(
    "webhook_endpoint_health_score",
    "Current health score of endpoints (0 to 100)",
    ["endpoint_url", "tenant_id"],
    registry=REGISTRY
)

# 6. Worker Execution Metrics
WORKER_TASKS_PROCESSED_TOTAL = Counter(
    "worker_tasks_processed_total",
    "Total number of Celery worker tasks processed",
    ["task_name", "status"],
    registry=REGISTRY
)

WORKER_TASK_DURATION = Histogram(
    "worker_task_duration_seconds",
    "Celery task duration in seconds",
    ["task_name"],
    buckets=(0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
    registry=REGISTRY
)

# RabbitMQ configuration
RABBITMQ_API_URL = os.getenv("RABBITMQ_API_URL", "http://rabbitmq:15672/api/queues/%2F")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")


def get_rabbitmq_queue_length(queue_name: str) -> int:
    """Queries RabbitMQ HTTP Management API to fetch queue message count."""
    try:
        response = httpx.get(
            f"{RABBITMQ_API_URL}/{queue_name}",
            auth=(RABBITMQ_USER, RABBITMQ_PASSWORD),
            timeout=1
        )
        if response.status_code == 200:
            return response.json().get("messages", 0)
    except Exception:
        pass
    return 0


def update_queue_length_metrics():
    """Queries RabbitMQ queue length metrics."""
    for queue_name in ["high_priority", "default", "low_priority"]:
        length = get_rabbitmq_queue_length(queue_name)
        WEBHOOK_QUEUE_LENGTH.labels(queue_name=queue_name).set(length)


async def update_database_metrics():
    """Queries PostgreSQL to fetch DLQ size and Endpoint health scores before scrape."""
    from app.core.database import AsyncSessionLocal
    from app.models.dead_letter_event import DeadLetterEvent
    from app.models.circuit_breaker import CircuitBreaker
    from sqlalchemy import select, func

    # 1. Update DLQ size
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(select(func.count(DeadLetterEvent.id)))
            WEBHOOK_DLQ_SIZE.set(result.scalar() or 0)
    except Exception:
        WEBHOOK_DLQ_SIZE.set(0)

    # 2. Update Endpoint Health scores
    try:
        async with AsyncSessionLocal() as session:
            result = await session.execute(select(CircuitBreaker))
            endpoints = result.scalars().all()
            for ep in endpoints:
                WEBHOOK_ENDPOINT_HEALTH_SCORE.labels(
                    endpoint_url=ep.endpoint_url,
                    tenant_id=str(ep.tenant_id)
                ).set(ep.health_score)
    except Exception:
        pass