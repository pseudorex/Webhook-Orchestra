"""
locustfile.py — Webhook Orchestra Load Test
============================================
Simulates real-world multi-tenant webhook traffic.

Scenarios:
  1. NormalUser      — Happy path: events delivered successfully
  2. RetryUser       — Events hitting a slow/502 endpoint → triggers retries
  3. PermanentFailUser — Events hitting 404 endpoint → goes straight to DLQ
  4. RateLimitUser   — Events hitting 429 endpoint → rate-limit backoff

Run:
  locust -f locustfile.py --host=http://localhost:8000
  Then open: http://localhost:8089
"""

import uuid
import random
import time
from locust import HttpUser, task, between, events


# ─────────────────────────────────────────────────────────────
# SCENARIO 1: Normal happy-path user
# Sends events to a working endpoint → successful delivery
# ─────────────────────────────────────────────────────────────
class NormalUser(HttpUser):
    weight = 5                    # 50% of all users
    wait_time = between(0.5, 2)
    api_key = None
    tenant_id = None
    last_event_id = None

    def on_start(self):
        """Register tenant + create working subscription"""
        res = self.client.post(
            "/tenants/register",
            json={
                "name": f"Normal Corp {uuid.uuid4().hex[:6]}",
                "email": f"normal_{uuid.uuid4().hex[:8]}@test.com",
                "webhook_url": "http://backend:8000/webhook"
            },
            name="/tenants/register"
        )
        if res.status_code != 200:
            return

        data = res.json()
        self.api_key = data["api_key"]
        self.tenant_id = data["id"]

        # Subscribe to working endpoint (self-loop: backend echoes back)
        self.client.post(
            "/subscriptions/",
            json={
                "tenant_id": self.tenant_id,
                "topic": "order.created",
                "endpoint_url": "http://backend:8000/webhook"
            },
            headers={"x-api-key": self.api_key},
            name="/subscriptions/"
        )

    @task(8)
    def send_event(self):
        """Send a normal event — most common action"""
        if not self.api_key:
            return

        res = self.client.post(
            "/events/",
            json={
                "event_type": "order.created",
                "payload": {
                    "order_id": uuid.uuid4().hex,
                    "amount": round(random.uniform(10, 5000), 2),
                    "currency": "USD",
                    "items": random.randint(1, 10)
                },
                "idempotency_key": uuid.uuid4().hex
            },
            headers={"x-api-key": self.api_key},
            name="/events/ [normal]"
        )
        if res.status_code == 200:
            self.last_event_id = res.json().get("id")

    @task(2)
    def check_event_status(self):
        """Check a specific event — simulates monitoring"""
        if not self.api_key or not self.last_event_id:
            return

        self.client.get(
            f"/events/{self.last_event_id}",
            headers={"x-api-key": self.api_key},
            name="/events/{id}"
        )

    @task(1)
    def check_endpoint_health(self):
        """Check circuit breaker health scores"""
        if not self.api_key:
            return

        self.client.get(
            "/endpoints/health",
            headers={"x-api-key": self.api_key},
            name="/endpoints/health"
        )


# ─────────────────────────────────────────────────────────────
# SCENARIO 2: Retry user
# Sends events to 503 endpoint → triggers retries + circuit breaker
# ─────────────────────────────────────────────────────────────
class RetryUser(HttpUser):
    weight = 2                    # 20% of all users
    wait_time = between(1, 3)
    api_key = None
    tenant_id = None

    def on_start(self):
        res = self.client.post(
            "/tenants/register",
            json={
                "name": f"Retry Corp {uuid.uuid4().hex[:6]}",
                "email": f"retry_{uuid.uuid4().hex[:8]}@test.com",
                "webhook_url": "https://httpstat.us/503"
            },
            name="/tenants/register"
        )
        if res.status_code != 200:
            return

        data = res.json()
        self.api_key = data["api_key"]
        self.tenant_id = data["id"]

        # Subscribe to a 503 endpoint — triggers TRANSIENT failures + retries
        self.client.post(
            "/subscriptions/",
            json={
                "tenant_id": self.tenant_id,
                "topic": "payment.failed",
                "endpoint_url": "https://httpstat.us/503"
            },
            headers={"x-api-key": self.api_key},
            name="/subscriptions/"
        )

    @task(5)
    def send_failing_event(self):
        """Event to 503 endpoint → TRANSIENT failure → retry with backoff"""
        if not self.api_key:
            return

        self.client.post(
            "/events/",
            json={
                "event_type": "payment.failed",
                "payload": {
                    "payment_id": uuid.uuid4().hex,
                    "reason": "insufficient_funds",
                    "amount": round(random.uniform(100, 2000), 2)
                },
                "idempotency_key": uuid.uuid4().hex
            },
            headers={"x-api-key": self.api_key},
            name="/events/ [retry-503]"
        )

    @task(2)
    def check_dlq(self):
        """Check DLQ — expect events here after max retries exhausted"""
        if not self.api_key:
            return

        self.client.get(
            "/dlq/",
            headers={"x-api-key": self.api_key},
            name="/dlq/"
        )


# ─────────────────────────────────────────────────────────────
# SCENARIO 3: Permanent failure user
# Sends events to 404 endpoint → goes straight to DLQ
# ─────────────────────────────────────────────────────────────
class PermanentFailUser(HttpUser):
    weight = 2                    # 20% of all users
    wait_time = between(2, 4)
    api_key = None
    tenant_id = None

    def on_start(self):
        res = self.client.post(
            "/tenants/register",
            json={
                "name": f"Dead Corp {uuid.uuid4().hex[:6]}",
                "email": f"dead_{uuid.uuid4().hex[:8]}@test.com",
                "webhook_url": "https://httpstat.us/404"
            },
            name="/tenants/register"
        )
        if res.status_code != 200:
            return

        data = res.json()
        self.api_key = data["api_key"]
        self.tenant_id = data["id"]

        # Subscribe to a 404 endpoint — permanent failure → straight to DLQ
        self.client.post(
            "/subscriptions/",
            json={
                "tenant_id": self.tenant_id,
                "topic": "invoice.created",
                "endpoint_url": "https://httpstat.us/404"
            },
            headers={"x-api-key": self.api_key},
            name="/subscriptions/"
        )

    @task(3)
    def send_permanent_fail_event(self):
        """Event to 404 → PERMANENT → DLQ immediately (no retry)"""
        if not self.api_key:
            return

        self.client.post(
            "/events/",
            json={
                "event_type": "invoice.created",
                "payload": {
                    "invoice_id": uuid.uuid4().hex,
                    "total": round(random.uniform(50, 10000), 2),
                    "due_date": "2026-12-31"
                },
                "idempotency_key": uuid.uuid4().hex
            },
            headers={"x-api-key": self.api_key},
            name="/events/ [dlq-404]"
        )

    @task(1)
    def replay_dead_event(self):
        """Try to replay a dead event from DLQ"""
        if not self.api_key:
            return

        # Check DLQ first
        dlq_res = self.client.get(
            "/dlq/",
            headers={"x-api-key": self.api_key},
            name="/dlq/"
        )

        if dlq_res.status_code == 200:
            dead_events = dlq_res.json()
            if dead_events:
                dead_id = dead_events[0]["id"]
                self.client.post(
                    f"/dlq/{dead_id}/replay",
                    headers={"x-api-key": self.api_key},
                    name="/dlq/{id}/replay"
                )


# ─────────────────────────────────────────────────────────────
# SCENARIO 4: Rate limit user
# Sends events to 429 endpoint → triggers rate-limit backoff
# ─────────────────────────────────────────────────────────────
class RateLimitUser(HttpUser):
    weight = 1                    # 10% of all users
    wait_time = between(0.2, 1)   # Fast sender
    api_key = None
    tenant_id = None

    def on_start(self):
        res = self.client.post(
            "/tenants/register",
            json={
                "name": f"Burst Corp {uuid.uuid4().hex[:6]}",
                "email": f"burst_{uuid.uuid4().hex[:8]}@test.com",
                "webhook_url": "https://httpstat.us/429"
            },
            name="/tenants/register"
        )
        if res.status_code != 200:
            return

        data = res.json()
        self.api_key = data["api_key"]
        self.tenant_id = data["id"]

        # Subscribe to a 429 endpoint — rate limit backoff (60s × attempt)
        self.client.post(
            "/subscriptions/",
            json={
                "tenant_id": self.tenant_id,
                "topic": "user.signup",
                "endpoint_url": "https://httpstat.us/429"
            },
            headers={"x-api-key": self.api_key},
            name="/subscriptions/"
        )

    @task
    def burst_events(self):
        """Rapid-fire events → triggers RATE_LIMITED backoff"""
        if not self.api_key:
            return

        self.client.post(
            "/events/",
            json={
                "event_type": "user.signup",
                "payload": {
                    "user_id": uuid.uuid4().hex,
                    "plan": random.choice(["free", "pro", "enterprise"])
                },
                "idempotency_key": uuid.uuid4().hex
            },
            headers={"x-api-key": self.api_key},
            name="/events/ [rate-limited-429]"
        )


# ─────────────────────────────────────────────────────────────
# Event hooks — printed to terminal during test
# ─────────────────────────────────────────────────────────────
@events.request.add_listener
def on_request(request_type, name, response_time, response_length, response, exception, **kwargs):
    if exception:
        print(f"[ERROR] {request_type} {name} | Exception: {exception}")


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    print("\n" + "="*60)
    print("  WEBHOOK ORCHESTRA — LOAD TEST STARTED")
    print("="*60)
    print("  Scenarios:")
    print("  - NormalUser    (50%) → successful deliveries")
    print("  - RetryUser     (20%) → 503 → retries + circuit breaker")
    print("  - PermanentFail (20%) → 404 → straight to DLQ")
    print("  - RateLimitUser (10%) → 429 → rate-limit backoff")
    print("="*60)
    print("  Watch metrics:")
    print("  RabbitMQ  → http://localhost:15672  (guest/guest)")
    print("  Grafana   → http://localhost:3000   (admin/admin)")
    print("  Jaeger    → http://localhost:16686")
    print("="*60 + "\n")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    print("\n" + "="*60)
    print("  LOAD TEST COMPLETE")
    print("  Take screenshots now from:")
    print("  1. Locust Web UI   → http://localhost:8089")
    print("  2. RabbitMQ        → http://localhost:15672")
    print("  3. Grafana         → http://localhost:3000")
    print("  4. Jaeger          → http://localhost:16686")
    print("="*60 + "\n")
