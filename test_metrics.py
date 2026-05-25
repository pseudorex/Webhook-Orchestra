from prometheus_client import generate_latest
from app.core.metrics import (
    REGISTRY,
    WEBHOOK_DELIVERIES_TOTAL,
    WEBHOOK_DELIVERY_LATENCY,
    WEBHOOK_RETRIES_TOTAL,
    WEBHOOK_DLQ_MOVES_TOTAL,
    WORKER_TASKS_PROCESSED_TOTAL,
    WORKER_TASK_DURATION,
    update_queue_length_metrics
)


def run_test():
    # Increment some metrics for testing
    WEBHOOK_DELIVERIES_TOTAL.labels(tenant_id="12", status="success", status_code="200").inc()
    WEBHOOK_DELIVERY_LATENCY.labels(tenant_id="12").observe(0.125)

    WEBHOOK_RETRIES_TOTAL.labels(tenant_id="12", retry_count="1").inc()
    WEBHOOK_DLQ_MOVES_TOTAL.labels(tenant_id="12", failure_type="consecutive_failures").inc()

    WORKER_TASKS_PROCESSED_TOTAL.labels(task_name="worker.tasks.deliver_webhook", status="success").inc()
    WORKER_TASK_DURATION.labels(task_name="worker.tasks.deliver_webhook").observe(0.45)

    # Check queue metrics helper
    update_queue_length_metrics()

    # Generate output
    output = generate_latest(REGISTRY).decode("utf-8")

    # Assertions
    assert "webhook_deliveries_total{status=\"success\",status_code=\"200\",tenant_id=\"12\"} 1.0" in output
    assert "webhook_delivery_latency_seconds_bucket{le=\"0.25\",tenant_id=\"12\"} 1.0" in output
    assert "webhook_retries_total{retry_count=\"1\",tenant_id=\"12\"} 1.0" in output
    assert "webhook_dlq_moves_total{failure_type=\"consecutive_failures\",tenant_id=\"12\"} 1.0" in output
    assert "worker_tasks_processed_total{status=\"success\",task_name=\"worker.tasks.deliver_webhook\"} 1.0" in output
    assert "worker_task_duration_seconds_bucket{le=\"0.5\",task_name=\"worker.tasks.deliver_webhook\"} 1.0" in output
    assert "webhook_queue_length" in output

    print("Prometheus metrics verification tests completed successfully!")


if __name__ == "__main__":
    run_test()