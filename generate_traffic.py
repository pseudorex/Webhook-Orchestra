import time
import uuid
import requests
import random

BASE_URL = "http://localhost:8000"

def run_load_test():
    print("[Traffic Generator] Starting detailed traffic generation for Grafana dashboard visualization...")
    print("Press Ctrl+C to stop traffic generation.")

    try:
        # 1. Register a Tenant
        tenant_payload = {
            "name": "Global Enterprise Corp",
            "email": f"enterprise_{uuid.uuid4().hex[:6]}@example.com",
            "webhook_url": "http://backend:8000/webhook"
        }
        tenant_resp = requests.post(f"{BASE_URL}/tenants/register", json=tenant_payload)
        tenant_resp.raise_for_status()
        tenant = tenant_resp.json()
        tenant_id = tenant["id"]
        api_key = tenant["api_key"]
        print(f"[Tenant] Registered Tenant ID: {tenant_id} (API Key: {api_key})")

        # 2. Register various subscriptions to trigger all Grafana metrics
        subscriptions = [
            {
                "topic": "order.created",
                "endpoint_url": "http://backend:8000/webhook",
                "desc": "Working endpoint (status 200)"
            },
            {
                "topic": "order.created",
                "endpoint_url": "https://httpstat.us/503",
                "desc": "Transient error (status 503, triggers retries & backoff)"
            },
            {
                "topic": "order.created",
                "endpoint_url": "https://httpstat.us/404",
                "desc": "Permanent error (status 404, triggers immediate DLQ)"
            },
            {
                "topic": "order.created",
                "endpoint_url": "https://httpstat.us/429",
                "desc": "Rate limited endpoint (status 429, triggers rate-limited backoff)"
            },
            {
                "topic": "order.created",
                "endpoint_url": "https://httpstat.us/200?sleep=1200",
                "desc": "Slow endpoint (status 200 with latency, degrades health score)"
            }
        ]

        headers = {"x-api-key": api_key} # ← API key header for authentication

        for sub in subscriptions:
            requests.post(
                f"{BASE_URL}/subscriptions/", 
                json={
                    "tenant_id": tenant_id,
                    "topic": sub["topic"],
                    "endpoint_url": sub["endpoint_url"]
                },
                headers=headers # ← Added authorization header
            ).raise_for_status()
            print(f"[Subscription] Subscribed to topic '{sub['topic']}' pointing to {sub['endpoint_url']} ({sub['desc']})")

        iteration = 1

        # Run traffic loop continuously
        while True:
            print(f"\n[Batch] Sending event batch #{iteration}...", end="", flush=True)
            # We send multiple events to generate nice rates and graphs
            for i in range(1, 11):
                event_payload = {
                    "event_type": "order.created",
                    "payload": {
                        "batch": iteration,
                        "event_index": i,
                        "amount": round(random.uniform(5.0, 1000.0), 2),
                        "items": ["widget", "gadget", "gizmo"],
                        "timestamp": time.time()
                    },
                    "idempotency_key": str(uuid.uuid4())
                }

                try:
                    requests.post(f"{BASE_URL}/events/", json=event_payload, headers=headers, timeout=5)
                    print(".", end="", flush=True)
                except Exception:
                    print("E", end="", flush=True)

                time.sleep(0.1) # Rapid fire to generate queue backlog

            iteration += 1
            # Sleep a bit between batches to avoid overloading, but enough to show metrics
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n[Traffic Generator] Stopped by user.")
    except Exception as e:
        print(f"\n[Error] Error during load test: {e}")

if __name__ == "__main__":
    run_load_test()