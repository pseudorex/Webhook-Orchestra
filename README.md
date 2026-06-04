# 🎻 Webhook Orchestra

> **A production-grade, distributed Webhook Delivery Engine** built with FastAPI, Celery, RabbitMQ, and PostgreSQL. Designed for high-throughput, fault-tolerant, multi-tenant webhook delivery with intelligent retry logic, circuit breaking, real-time observability, and dead-letter queue recovery.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [System Architecture](#system-architecture)
- [Tech Stack](#tech-stack)
- [Request Flow](#request-flow)
- [Reliability Engine](#reliability-engine)
- [Queue Architecture](#queue-architecture)
- [API Reference](#api-reference)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Load Test Results](#load-test-results)
- [Observability](#observability)
- [Project Structure](#project-structure)

---

## Overview

Webhook Orchestra solves the hard problem of **reliable webhook delivery at scale**. When your platform emits an event, the system guarantees that every registered subscriber receives it — even if their endpoint is temporarily down, rate-limiting requests, or experiencing intermittent failures.

The system is built around three core principles:

| Principle | How It's Implemented |
|-----------|---------------------|
| **Never lose an event** | All events persisted to PostgreSQL before any delivery attempt |
| **Smart failure handling** | Failures are classified and retried with appropriate strategies |
| **Self-healing infrastructure** | Circuit breakers automatically detect and recover from outages |

---

## Key Features

### 1. 🏢 Multi-Tenant Architecture
- Each tenant gets a unique **API key** and **webhook signing secret** on registration
- All data (events, subscriptions, deliveries) is **tenant-scoped** — complete isolation
- Tenants cannot access or replay each other's events
- Idempotency keys prevent duplicate event processing per tenant

### 2. 📡 Topic-Based Fan-Out
- Events are published to a **topic** (e.g., `payment.created`, `order.shipped`)
- All active subscriptions matching that topic receive the event
- A single event fans out to **multiple endpoints concurrently** via Celery tasks
- Each fan-out creates an independent `SubscriptionDelivery` record for tracking

### 3. 🔐 Cryptographic Payload Signing
- Every webhook payload is signed with **HMAC-SHA256** using the tenant's secret
- The signature is sent in the `X-Webhook-Signature` header
- Receivers can verify authenticity by recomputing the signature
- JSON payload is serialized with **sorted keys** to ensure consistent signatures

### 4. 🔁 Intelligent Retry System
Failures are classified and retried with strategies specific to the failure type:

| Failure Type | HTTP Codes | Retry Strategy | Max Retries |
|-------------|------------|---------------|-------------|
| `PERMANENT` | 400, 401, 403, 404 | No retry → straight to DLQ | 0 |
| `TRANSIENT` | 5xx | Exponential backoff (2ⁿ + jitter) | 5 |
| `RATE_LIMITED` | 429 | Linear backoff (60s × attempt) | 10 |
| `TIMEOUT` | Request timeout | Linear backoff (2s × attempt) | 5 |
| `DNS_ERROR` | DNS failure | Linear backoff (5s × attempt) | 5 |
| `CONNECTION_REFUSED` | Connection refused | Exponential backoff | 5 |

### 5. ⚡ Circuit Breaker
Three-state machine that automatically protects against cascading failures:

```
         5 failures              cooldown expires
CLOSED ─────────────► OPEN ──────────────────────► HALF_OPEN
  ▲                                                     │
  │                    test request succeeds             │
  └─────────────────────────────────────────────────────┘
         (if test fails → back to OPEN)
```

- **CLOSED** → Normal operation, all requests allowed
- **OPEN** → Endpoint is blocked, requests are rescheduled with countdown
- **HALF_OPEN** → One test request allowed to probe recovery
- Health score (0–100) computed from success rate, latency, and failure streaks

### 6. 📬 Dead Letter Queue (DLQ)
- Events that exhaust all retries or are permanently rejected are moved to the DLQ
- Full failure context stored: `failure_type`, `final_error`, `retry_count`
- Manual replay via API: resets retry count and re-queues to `low_priority`
- DLQ is tenant-scoped — each tenant can only see and replay their own dead events

### 7. 🎚️ Adaptive Queue Routing
- Monitors RabbitMQ `high_priority` queue length via the RabbitMQ HTTP Management API
- If backlog exceeds **2000 tasks**, new events are demoted to `default` queue
- Prevents priority starvation during traffic spikes
- Configurable congestion threshold via environment variable

### 8. 📊 Full Observability Stack
- **Prometheus** metrics: delivery attempts, latency histograms, retry counts, DLQ size, queue lengths, worker task duration
- **Grafana** dashboard: real-time panels for throughput, latency percentiles, health scores, and failure breakdowns
- **Jaeger** (OpenTelemetry): distributed traces across FastAPI → Celery → HTTP delivery
- **Structured JSON logs** with `correlation_id`, `tenant_id`, and `event_id` on every line

---

## System Architecture

```
                         ┌──────────────────────────────────┐
                         │         CLIENT / API CALLER       │
                         └──────────────┬───────────────────┘
                                        │ POST /events/
                                        │ X-API-Key: <tenant_key>
                                        ▼
                         ┌──────────────────────────────────┐
                         │        FastAPI Backend            │
                         │  ┌─────────────────────────────┐ │
                         │  │  LoggingContextMiddleware    │ │  ← Sets correlation_id,
                         │  │  (X-Correlation-ID)         │ │    tenant_id, event_id
                         │  └─────────────────────────────┘ │
                         │  ┌─────────────────────────────┐ │
                         │  │  Auth → Idempotency Check   │ │  ← Validates API key,
                         │  │  → Create Event Row (DB)    │ │    prevents duplicates
                         │  └─────────────────────────────┘ │
                         │  ┌─────────────────────────────┐ │
                         │  │     RoutingEngine           │ │
                         │  │     .fan_out_event()        │ │  ← Finds all matching
                         │  │                             │ │    subscriptions
                         │  │  For each subscription:     │ │
                         │  │  • Create SubscriptionDelivery│ │
                         │  │  • Check adaptive queue     │ │  ← Queries RabbitMQ API
                         │  │  • apply_async(delivery.id) │ │  ← Publishes to RabbitMQ
                         │  └─────────────────────────────┘ │
                         └──────────────┬───────────────────┘
                                        │
                     ┌──────────────────┼──────────────────┐
                     ▼                  ▼                   ▼
            ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐
            │ high_priority│  │   default    │  │  low_priority    │
            │    queue     │  │    queue     │  │     queue        │
            │  (new events)│  │  (retries 1-2│  │  (retries 3+,   │
            │              │  │   attempts)  │  │   DLQ replays)   │
            └──────┬───────┘  └──────┬───────┘  └────────┬─────────┘
                   │                 └──────────────┬──────┘
                   ▼                                ▼
        ┌─────────────────────┐        ┌─────────────────────────┐
        │   worker-high       │        │     worker-retry         │
        │  concurrency: 32    │        │     concurrency: 16      │
        │  threads pool       │        │     threads pool         │
        └──────────┬──────────┘        └────────────┬────────────┘
                   └────────────────┬───────────────┘
                                    ▼
                   ┌─────────────────────────────────────┐
                   │         WebhookEngine               │
                   │         .process_event()            │
                   │                                     │
                   │  1. Circuit Breaker Check           │
                   │  2. Build + Sign Payload            │
                   │  3. HTTP POST to endpoint           │
                   │  4. Measure latency                 │
                   │                                     │
                   │  ┌─────────────┐ ┌───────────────┐ │
                   │  │  SUCCESS    │ │    FAILURE     │ │
                   │  │  2xx        │ │  4xx/5xx/      │ │
                   │  │             │ │  timeout/dns   │ │
                   │  │ • delivered │ │                │ │
                   │  │ • circuit   │ │ FailureClassifier│
                   │  │   record    │ │ .classify()    │ │
                   │  │   success   │ │                │ │
                   │  └─────────────┘ │ Retryable?     │ │
                   │                  │  YES → retry   │ │
                   │                  │  NO  → DLQ     │ │
                   │                  └───────────────┘ │
                   └─────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Web Framework** | FastAPI 0.136 | Async HTTP API server |
| **Task Queue** | Celery 5.6 | Distributed task processing |
| **Message Broker** | RabbitMQ 3 | Queue storage and delivery |
| **Database** | PostgreSQL + SQLAlchemy | Persistent event/delivery storage |
| **Async DB Driver** | asyncpg | FastAPI async database access |
| **Sync DB Driver** | psycopg2 | Celery worker database access |
| **Migrations** | Alembic | Database schema versioning |
| **Metrics** | Prometheus + Grafana | Real-time performance monitoring |
| **Tracing** | OpenTelemetry + Jaeger | Distributed request tracing |
| **Containerization** | Docker + Docker Compose | Full stack orchestration |

---

## Request Flow

### Flow 1: Publishing an Event

```
POST /events/  { event_type, payload, idempotency_key }
       │
       ├── [AUTH] Validate X-API-Key → load Tenant
       ├── [IDEMPOTENCY] Check idempotency_key → return cached if exists
       ├── [PERSIST] INSERT INTO events (tenant_id, event_type, payload)
       │
       └── RoutingEngine.fan_out_event()
             │
             ├── SELECT subscriptions WHERE tenant_id=X AND topic=event_type
             │
             └── For each subscription:
                   ├── INSERT INTO subscription_deliveries (pending)
                   ├── get_adaptive_queue("high_priority")
                   │     └── GET RabbitMQ API: /api/queues/%2F/high_priority
                   │           → if messages > 2000: use "default"
                   │           → else: use "high_priority"
                   └── deliver_webhook.apply_async(args=[delivery.id])
                         └── Message published to RabbitMQ ✓

Response to client: 200 OK { id, event_type, status: "received" }
(delivery happens asynchronously)
```

### Flow 2: Webhook Delivery (Worker Side)

```
RabbitMQ delivers message → { task: deliver_webhook, args: [42] }
       │
       ▼
worker/tasks.py → deliver_webhook(subscription_delivery_id=42)
       │
       ├── db.get(SubscriptionDelivery, 42)  ← load delivery record
       ├── db.get(Event, delivery.event_id)   ← load event
       ├── tenant_id_var.set(event.tenant_id) ← set logging context
       │
       └── WebhookEngine.process_event(db, delivery)
             │
             ├── [CIRCUIT BREAKER] CircuitBreakerService.should_allow_request()
             │     OPEN + cooldown not expired → schedule retry → return
             │
             ├── [BUILD] payload = { event_id, event_type, payload }
             │
             ├── [SIGN] HMAC-SHA256(sorted_json(payload), tenant.webhook_secret)
             │
             ├── [DELIVER] requests.post(endpoint_url, json=payload, timeout=10)
             │
             ├── [SUCCESS 2xx]
             │     delivery.status = "delivered"
             │     CircuitBreakerService.record_success(latency_ms)
             │     Prometheus metrics updated
             │
             └── [FAILURE]
                   FailureClassifier.classify(status_code or exception)
                   CircuitBreakerService.record_failure(latency_ms)
                   handle_failure() →
                     PERMANENT or max retries exceeded → DLQ
                     else → RetryPolicy.get_delay() → Backoff
                            deliver_webhook.apply_async(countdown=delay)
```

### Flow 3: DLQ Replay

```
POST /events/{id}/replay   or   POST /dlq/{id}/replay
       │
       ├── Verify event.tenant_id == authenticated tenant
       │
       └── replay_service.replay_event()
             │
             ├── SELECT delivery WHERE event_id=X AND status="dead"
             ├── delivery.status = "retrying"
             ├── delivery.retry_count = 0        ← RESET
             ├── delivery.failure_type = None
             ├── celery.send_task(deliver_webhook, queue="low_priority")
             └── event.replay_count += 1
```

---

## Reliability Engine

### Failure Classification

```python
# app/services/reliability/failure_classifier.py

classify(status_code=404) → PERMANENT      # No retry, straight to DLQ
classify(status_code=503) → TRANSIENT      # Exponential backoff retry
classify(status_code=429) → RATE_LIMITED   # 60s × attempt backoff
classify(error=TimeoutError) → TIMEOUT     # 2s × attempt backoff
classify(error=DNSError) → DNS_ERROR       # 5s × attempt backoff
```

### Backoff Strategies

```python
# app/services/reliability/backoff.py

Exponential(attempt):    2^attempt + random(0,1)   # 2s, 4s, 8s, 16s, 32s
Timeout(attempt):        2 × attempt               # 2s, 4s, 6s, 8s, 10s
DNS(attempt):            5 × attempt               # 5s, 10s, 15s, 20s, 25s
RateLimit(attempt):      60 × attempt              # 60s, 120s, 180s...
```

### Circuit Breaker State Machine

```
State transitions:
  CLOSED  → OPEN:       failure_count >= 5
  OPEN    → HALF_OPEN:  cooldown_seconds elapsed (default: 60s)
  HALF_OPEN → CLOSED:   next request succeeds
  HALF_OPEN → OPEN:     next request fails

Health Score Formula:
  score = 100
  score -= (consecutive_failures × 10)
  score -= (latency_ms > 5000) × 20      # penalise slow endpoints
  score -= (success_rate < 0.95) × 15   # penalise low success rate
  score = max(0, min(100, score))

  score ≥ 80  → "healthy"
  score 50-79 → "degraded"
  score < 50  → "unhealthy"
```

---

## Queue Architecture

Three dedicated queues prevent retry storms from starving new traffic:

```
Queue           Worker          Concurrency    Purpose
─────────────   ─────────────   ───────────    ────────────────────────────
high_priority   worker-high     32 threads     Brand new events (first attempt)
default         worker-retry    16 threads     Active retries (attempt 1-2)
low_priority    worker-retry    16 threads     Late retries (attempt 3+), DLQ replays
```

**Retry Demotion Logic:**
```python
# In handle_failure():
target_queue = "default" if delivery.retry_count <= 2 else "low_priority"
```

**Adaptive Routing:**
```python
# In get_adaptive_queue():
if queue == "high_priority" and rabbitmq_queue_length > 2000:
    return "default"   # demote to prevent starvation
```

---

## API Reference

### Authentication
All endpoints (except `/tenants/register`) require:
```
X-API-Key: <tenant_api_key>
```

---

### Tenants

#### `POST /tenants/register`
Register a new tenant. Returns API key and webhook secret.

**Request:**
```json
{
  "name": "Acme Corp",
  "email": "acme@example.com",
  "webhook_url": "https://acme.com/webhook"
}
```

**Response:**
```json
{
  "id": 1,
  "name": "Acme Corp",
  "api_key": "a1b2c3d4...",
  "webhook_secret": "secret_xyz...",
  "webhook_url": "https://acme.com/webhook"
}
```

---

### Subscriptions

#### `POST /subscriptions/`
Subscribe an endpoint to receive events for a specific topic.

**Request:**
```json
{
  "tenant_id": 1,
  "topic": "payment.created",
  "endpoint_url": "https://acme.com/webhook/payments"
}
```

#### `GET /subscriptions/`
List all subscriptions for the authenticated tenant.

---

### Events

#### `POST /events/`
Publish a new event. Triggers fan-out to all matching subscriptions.

**Request:**
```json
{
  "event_type": "payment.created",
  "payload": {
    "amount": 500,
    "currency": "USD",
    "order_id": "ord_001"
  },
  "idempotency_key": "unique-key-per-event"
}
```

**Response:**
```json
{
  "id": 101,
  "tenant_id": 1,
  "event_type": "payment.created",
  "status": "received",
  "created_at": "2026-06-04T10:00:00Z"
}
```

#### `GET /events/{event_id}`
Get details of a specific event (tenant-scoped).

#### `POST /events/{event_id}/replay`
Replay all dead deliveries for a specific event.

---

### Dead Letter Queue

#### `GET /dlq/`
Fetch all dead events for the authenticated tenant.

**Response:**
```json
[
  {
    "id": 1,
    "original_event_id": 101,
    "subscription_delivery_id": 42,
    "failure_type": "TRANSIENT",
    "final_error": "503 Service Unavailable",
    "replay_count": 0,
    "failed_at": "2026-06-04T10:05:30Z"
  }
]
```

#### `POST /dlq/{dead_event_id}/replay`
Replay a specific dead event. Resets retry count and re-queues to `low_priority`.

**Response:**
```json
{
  "message": "Replay scheduled",
  "delivery_id": 42
}
```

---

### Monitoring

#### `GET /endpoints/health`
Real-time circuit breaker health for all endpoints of the authenticated tenant.

**Response:**
```json
[
  {
    "endpoint_url": "https://acme.com/webhook",
    "state": "closed",
    "health_score": 88.5,
    "failure_count": 0,
    "success_rate": 0.97,
    "average_latency_ms": 312,
    "last_checked": "2026-06-04T12:00:00Z"
  },
  {
    "endpoint_url": "https://bad-endpoint.com/hook",
    "state": "open",
    "health_score": 0,
    "failure_count": 12,
    "success_rate": 0.0,
    "average_latency_ms": 10001,
    "last_checked": "2026-06-04T12:00:00Z"
  }
]
```

#### `GET /metrics`
Prometheus metrics endpoint (scraped every 5 seconds).

#### `GET /webhook`
Internal webhook receiver with idempotency guard and signature verification.

---

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.12+ (for local development)

### 1. Clone & Configure

```bash
git clone <repository-url>
cd webhook-orchestra
cp .env.example .env
# Edit .env with your PostgreSQL credentials
```

### 2. Start the Full Stack

```bash
docker-compose up -d --build
```

This starts:
| Service | Port | Description |
|---------|------|-------------|
| FastAPI Backend | 8000 | Main API server |
| Celery Worker High | 8001 | Processes high_priority queue |
| Celery Worker Retry | 8002 | Processes default + low_priority queues |
| RabbitMQ | 5672, 15672 | Message broker + management UI |
| PostgreSQL | 5432 | Persistent storage |
| Prometheus | 9090 | Metrics collection |
| Grafana | 3000 | Monitoring dashboard |
| Jaeger | 16686 | Distributed tracing |

### 3. Run Migrations

```bash
docker exec -it webhook_backend alembic upgrade head
```

### 4. Verify Installation

```bash
# Check API docs
open http://localhost:8000/docs

# Check RabbitMQ management
open http://localhost:15672   # guest / guest

# Check Grafana dashboard
open http://localhost:3000    # admin / admin

# Check Jaeger traces
open http://localhost:16686
```

### 5. Register Your First Tenant & Send an Event

```bash
# Register tenant
curl -X POST http://localhost:8000/tenants/register \
  -H "Content-Type: application/json" \
  -d '{"name": "My App", "email": "me@example.com", "webhook_url": "https://myapp.com/hook"}'

# Copy the api_key from response, then create a subscription
curl -X POST http://localhost:8000/subscriptions/ \
  -H "Content-Type: application/json" \
  -H "x-api-key: YOUR_API_KEY" \
  -d '{"tenant_id": 1, "topic": "order.created", "endpoint_url": "https://myapp.com/hook"}'

# Send an event
curl -X POST http://localhost:8000/events/ \
  -H "Content-Type: application/json" \
  -H "x-api-key: YOUR_API_KEY" \
  -d '{"event_type": "order.created", "payload": {"order_id": "123"}, "idempotency_key": "evt-001"}'
```

### 6. Run Load Test

```bash
# Install Locust
pip install locust

# Run with Web UI
locust -f locustfile.py --host=http://localhost:8000
# Open http://localhost:8089 → set 100 users, 10 spawn rate
```

---

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `DATABASE_URL` | `postgresql+asyncpg://...` | Async PostgreSQL connection |
| `RABBITMQ_API_URL` | `http://rabbitmq:15672/api/queues/%2F` | RabbitMQ management API |
| `RABBITMQ_USER` | `guest` | RabbitMQ username |
| `RABBITMQ_PASSWORD` | `guest` | RabbitMQ password |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://jaeger:4317` | Jaeger OTLP endpoint |
| `OTEL_SERVICE_NAME` | `webhook-backend` | Service name for traces |
| `METRICS_PORT` | `8001` / `8002` | Worker Prometheus metrics port |

---

## Load Test Results

Load tested using **Locust** with 4 concurrent user scenarios over 10+ minutes.

### Test Scenarios

| Scenario | Weight | Target Endpoint | Purpose |
|----------|--------|----------------|---------|
| `NormalUser` | 50% | `http://backend:8000/webhook` | Successful deliveries |
| `RetryUser` | 20% | `https://httpstat.us/503` | Retry + circuit breaker |
| `PermanentFailUser` | 20% | `https://httpstat.us/404` | DLQ capture |
| `RateLimitUser` | 10% | `https://httpstat.us/429` | Rate-limit backoff |

### Results Summary

| Metric | Value |
|--------|-------|
| **Total Requests** | 19,940 |
| **Total Failures** | 4 (0.02%) |
| **Throughput** | 29.8 req/sec sustained |
| **Median Response Time** | 1,300 ms |
| **95th Percentile** | 3,200 ms |
| **99th Percentile** | 4,700 ms |
| **Average Response Time** | 1,456 ms |
| **Min Response Time** | 57 ms |
| **Max Response Time** | 9,697 ms |

### Per-Endpoint Breakdown

| Endpoint | Requests | Failures | Failure % | Median (ms) | p95 (ms) |
|----------|----------|----------|-----------|-------------|----------|
| `POST /events/ [normal]` | 8,018 | 0 | **0%** | 1,500 | 3,500 |
| `POST /events/ [rate-limited-429]` | 2,617 | 0 | **0%** | 1,400 | 3,400 |
| `POST /events/ [retry-503]` | 2,457 | 0 | **0%** | 1,600 | 3,500 |
| `POST /events/ [dlq-404]` | 1,946 | 3 | 0.15% | 1,600 | 3,600 |
| `GET /dlq/` | 1,665 | 1 | 0.06% | 570 | 1,500 |
| `GET /events/{id}` | 1,983 | 0 | **0%** | 520 | 1,500 |
| `GET /endpoints/health` | 1,054 | 0 | **0%** | 540 | 1,500 |
| `POST /subscriptions/` | 100 | 0 | **0%** | 360 | 1,800 |
| `POST /tenants/register` | 100 | 0 | **0%** | 580 | 1,600 |

### Reliability Metrics (Post Load Test)

| Metric | Value | Interpretation |
|--------|-------|---------------|
| DLQ Backlog Size | **39** | All failures captured, nothing lost |
| High Priority Queue | **0** | Fully processed |
| Default Queue | **0** | Fully processed |
| Low Priority Queue | **0** | Fully processed |
| Healthy Endpoint Health Score | **88.5 / 100** | Circuit breaker correctly tracking |
| Failing Endpoint Health Score | **0 / 100** | Circuit breaker opened for bad endpoints |
| DLQ Failure Types | **TIMEOUT 50% / TRANSIENT 50%** | Failure classification working correctly |

---

## Observability

### Prometheus Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `webhook_delivery_attempts_total` | Counter | Total delivery attempts by `status_code`, `tenant_id`, `error_class` |
| `webhook_delivery_latencies_seconds` | Histogram | HTTP delivery latency (p50, p95, p99) by `tenant_id` |
| `webhook_retries_total` | Counter | Retry schedule count by `tenant_id`, `retry_count` |
| `webhook_dlq_moves_total` | Counter | DLQ move count by `tenant_id`, `failure_type` |
| `webhook_dlq_size` | Gauge | Current number of events in DLQ |
| `webhook_queue_length` | Gauge | RabbitMQ queue length by `queue_name` |
| `webhook_endpoint_health_score` | Gauge | Circuit breaker health score by `endpoint_url`, `tenant_id` |
| `worker_tasks_processed_total` | Counter | Celery task count by `task_name`, `status` |
| `worker_task_duration_seconds` | Histogram | Celery task duration by `task_name` |

### Grafana Dashboard Panels

- **DLQ Current Backlog Size** — gauge showing live DLQ count
- **Queue Backlogs** — live stat panels for high/default/low priority queues
- **Endpoint Health Scores** — bar gauge showing circuit breaker health per endpoint
- **Failures - DLQ Moves (Total)** — total DLQ moves over dashboard time range
- **Failures - DLQ Failure Types** — donut chart breaking down failure classification
- **Throughput - Delivery Attempts per Second** — time series by status code
- **Throughput - Latency Percentiles** — p50/p95/p99 delivery latency
- **Retries Scheduled per Minute** — time series by tenant and retry attempt number
- **Worker Task Duration (p95)** — Celery worker processing time

### Jaeger Distributed Traces

Every webhook delivery generates a trace spanning:
```
POST /events/  [FastAPI]
  └── fan_out_event  [RoutingEngine]
  └── deliver_webhook  [Celery Worker]
        └── POST https://endpoint.com/webhook  [HTTP]
              └── response time, status code
```

Access traces at `http://localhost:16686` → Select service `webhook-backend` or `webhook-worker-high`.

---

## Project Structure

```
webhook-orchestra/
│
├── app/                              # FastAPI application
│   ├── main.py                       # App entry point, middleware, routes
│   ├── api/
│   │   ├── dependencies/
│   │   │   └── auth.py               # API key authentication
│   │   └── routes/
│   │       ├── events.py             # Event publishing + replay
│   │       ├── subscriptions.py      # Subscription management
│   │       ├── tenants.py            # Tenant registration
│   │       ├── dlq.py                # Dead letter queue endpoints
│   │       ├── replay.py             # Event-level replay
│   │       ├── endpoint_health.py    # Circuit breaker health
│   │       └── webhook_receiver.py   # Incoming webhook receiver
│   ├── core/
│   │   ├── database.py               # Async SQLAlchemy setup (asyncpg)
│   │   ├── logging.py                # Structured JSON logger + Celery signals
│   │   └── metrics.py                # All Prometheus metric definitions
│   ├── models/                       # SQLAlchemy ORM models
│   │   ├── tenant.py
│   │   ├── event.py
│   │   ├── subscription.py
│   │   ├── subscription_delivery.py
│   │   ├── circuit_breaker.py
│   │   ├── dead_letter_event.py
│   │   └── processed_webhook.py
│   └── services/
│       ├── routing_engine.py         # Fan-out + adaptive queue routing
│       ├── signature_service.py      # HMAC-SHA256 payload signing
│       ├── replay_service.py         # DLQ replay orchestration
│       ├── subscription_service.py   # Subscription CRUD
│       └── reliability/
│           ├── webhook_engine.py     # Core delivery orchestrator
│           ├── circuit_breaker_service.py  # State machine
│           ├── failure_classifier.py       # Error classification
│           ├── retry_policy.py             # Max retries per failure type
│           ├── backoff.py                  # Delay calculation strategies
│           └── dlq_service.py             # DLQ move operations
│
├── worker/
│   ├── celery_app.py                 # Celery + RabbitMQ configuration
│   ├── database.py                   # Sync SQLAlchemy setup (psycopg2)
│   └── tasks.py                      # deliver_webhook Celery task
│
├── migration/                        # Alembic DB migrations
│   └── versions/
│
├── provisioning/                     # Grafana auto-provisioning
│   ├── dashboards/
│   │   └── webhook_orchestra.json    # Dashboard definition
│   └── datasources/
│
├── locustfile.py                     # Load test scenarios
├── generate_traffic.py               # Simple traffic generator
├── prometheus.yml                    # Prometheus scrape config
├── docker-compose.yml                # Full stack orchestration
├── Dockerfile
├── requirements.txt
└── .env
```

---

## How Correlation IDs Flow Through the System

Every request is tagged with a `correlation_id` that propagates through the entire system:

```
Client Request
  │  X-Correlation-ID: "abc-123"  (or auto-generated UUID)
  ▼
LoggingContextMiddleware (FastAPI)
  │  correlation_id_var.set("abc-123")
  │  tenant_id_var.set(None)     ← filled by auth
  │  event_id_var.set(None)      ← filled by events route
  ▼
Route Handler → Auth → Service → DB
  │  All log lines automatically include:
  │  { "correlation_id": "abc-123", "tenant_id": 5, "event_id": 101 }
  ▼
Celery Task (before_task_publish signal)
  │  Headers injected: { "correlation_id": "abc-123", "tenant_id": 5 }
  ▼
Celery Worker (task_prerun signal)
  │  correlation_id_var.set from message headers
  │  All worker log lines also tagged with "abc-123"
  ▼
Response Header: X-Correlation-ID: "abc-123"
  └── Client can reference this ID for debugging
```

---

## Security Considerations

1. **API Key Hashing** — API keys are stored as hashed values; raw keys are only returned once on registration
2. **Tenant Isolation** — All DB queries are scoped to `tenant_id`; cross-tenant access returns 404
3. **Payload Signing** — HMAC-SHA256 with per-tenant secrets; sorted JSON keys prevent signature manipulation
4. **No Secret Logging** — Webhook secrets are never logged; only tenant IDs appear in logs
5. **Idempotency Guard** — Duplicate events are rejected at ingestion; duplicate webhooks rejected at reception

---

*Built with ❤️ using FastAPI, Celery, RabbitMQ, and PostgreSQL*
