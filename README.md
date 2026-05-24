# Webhook Orchestra 🎵
Webhook Orchestra is a production-grade, highly reliable Webhook Delivery Engine built with **FastAPI**, **Celery**, and **Redis**. It is designed to handle scalable event fan-out, intelligent retries, automatic circuit breaking, endpoint health monitoring, and dynamic queue load balancing.
---
## 🚀 Key Features
### 1. Multi-Tenant Onboarding & Security
* **Tenant Registration:** Register tenants to generate unique API keys (`X-API-Key`) and webhook signing secrets.
* **Payload Signing:** Webhooks are cryptographically signed using `HMAC-SHA256` signatures (`X-Webhook-Signature`) to allow receivers to verify authenticity.
* **Idempotency keys:** Prevent duplicate events by enforcing tenant-scoped idempotency keys.
### 2. Fan-Out Routing & Subscription System
* **Topic-Based Routing:** Register multiple subscriptions to route events based on event topics (e.g., `order.created`, `payment.failed`).
* **Multi-Destination Fan-out:** A single event is fanned out concurrently to all active subscriptions.
### 3. Reliability & Failure Classification
* **Classified Retries:** Differentiates between transient failures (5xx), permanent failures (4xx), timeouts, connection refused, and rate limits.
* **Smart Backoffs:** Custom retry policies per failure class (e.g., Exponential Backoff for transient errors, Rate-Limit backoff, and specialized Timeout backoffs).
* **Dead Letter Queue (DLQ):** Exhausted or permanently failing events are safely quarantined in a DLQ.
* **Manual Replay:** Administrative API endpoints to replay failed events from the DLQ.
### 4. Circuit Breaker (Abuse & Resource Protection)
* **Outage Detection:** Trips to `open` after 5 consecutive endpoint failures.
* **Cooldown Protection:** When the circuit is `open`, requests to that endpoint are blocked immediately (preventing request overloading) and rescheduled.
* **Self-Healing (Half-Open):** After a 60-second cooldown, sends a single test probe request. If the endpoint recovers, the circuit closes; otherwise, it re-opens.
### 5. Real-Time Endpoint Health Monitoring
* **Success Rate & Latency:** Computes rolling average response times and success rates.
* **Health Scoring:** Assigns a score (0 to 100) based on consecutive failures, latency excess, and success rate drops.
* **State Engine:** Automatically categorizes endpoints into `healthy` (score >= 80), `degraded` (score 50-79), or `unhealthy` (score < 50).
### 6. Queue Partitioning & Worker Specialization
* **Worker Isolation:** Webhooks are divided into three queues to prevent retries from starving new traffic:
  * `high_priority`: Brand new events (handled by a dedicated `worker-high` service).
  * `default`: Active retries (handled by `worker-retry`).
  * `low_priority`: Degraded retries and DLQ replays (handled by `worker-retry`).
### 7. Adaptive Queue Routing (Load Balancing)
* **Backlog Detection:** Queries Redis lengths dynamically before routing tasks.
* **Congestion Demotion:** If the `high_priority` queue becomes congested (backlog > 10 items), new events are dynamically routed to the `default` queue to keep processing active.
---
## 🛠️ System Architecture
```
                       [ FastAPI HTTP Backend ]
                                  │
                       ( Routing Engine & SLA )
                                  │
                 ┌────────────────┼────────────────┐
                 ▼                ▼                ▼
         [ high_priority ]    [ default ]   [ low_priority ]
                 │                │                │
                 ▼                └────────┬───────┘
          [ worker-high ]                  ▼
         (4 Concurrency)            [ worker-retry ]
                                    (2 Concurrency)
```
---
## 📂 API Reference
### Tenants
* `POST /tenants/register`: Register a new tenant.
### Subscriptions
* `POST /subscriptions/`: Subscribe an endpoint to a topic.
### Events
* `POST /events/`: Publish a new event (requires `X-API-Key` header).
* `POST /events/{event_id}/replay`: Replay a specific event.
### Dead Letter Queue (DLQ)
* `GET /dlq/`: Fetch all quarantined dead events.
* `POST /dlq/{dead_event_id}/replay`: Replay a specific dead event.
### Monitoring
* `GET /endpoints/health`: Fetch real-time health stats for all endpoints.
---
## 🐳 Quick Start
1. **Spin up the stack (Backend, Workers, Redis):**
   ```bash
   docker-compose up --build
   ```
2. **Run database migrations:**
   ```bash
   docker exec -it webhook_backend alembic upgrade head
   ```
3. **Verify running API:**
   Access the documentation in your browser at `http://localhost:8000/docs`.
