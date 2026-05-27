import time
import uuid
import requests
import random

BASE_URL = "http://localhost:8000"


def run_load_test():
    print("🚀 Starting traffic generation...")

    # 1. Register a Tenant
    tenant_payload = {
        "name": "Acme Corp",
        "email": f"acme_{uuid.uuid4().hex[:6]}@example.com",
        "webhook_url": "http://backend:8000/webhook"
    }
    tenant_resp = requests.post(f"{BASE_URL}/tenants/register", json=tenant_payload)
    tenant_resp.raise_for_status()
    tenant = tenant_resp.json()
    tenant_id = tenant["id"]
    api_key = tenant["api_key"]
    print(f"✅ Registered Tenant ID: {tenant_id} (API Key: {api_key})")

    # 2. Subscribe to "order.created" (Successful endpoint)
    success_sub = {
        "tenant_id": tenant_id,
        "topic": "order.created",
        "endpoint_url": "http://backend:8000/webhook"  # Routes back to our app's mock receiver
    }
    requests.post(f"{BASE_URL}/subscriptions/", json=success_sub).raise_for_status()
    print("✅ Created working subscription pointing to: /webhook")

    # 3. Subscribe to "order.created" (Failing endpoint to trigger retries and DLQ)
    failing_sub = {
        "tenant_id": tenant_id,
        "topic": "order.created",
        "endpoint_url": "https://httpstat.us/400"  # Will trigger immediate DLQ move
    }
    requests.post(f"{BASE_URL}/subscriptions/", json=failing_sub).raise_for_status()
    print("✅ Created failing subscription pointing to: httpstat.us/400 (will cause immediate DLQ)")

    # 4. Publish events
    headers = {"x-api-key": api_key}
    for i in range(1, 41):
        event_payload = {
            "event_type": "order.created",
            "payload": {
                "order_id": i,
                "amount": round(random.uniform(10.0, 500.0), 2),
                "items": ["widget", "gadget"]
            },
            "idempotency_key": str(uuid.uuid4())
        }

        try:
            print(f"📨 Sending event #{i}...")
            requests.post(f"{BASE_URL}/events/", json=event_payload, headers=headers)
        except Exception as e:
            print(f"❌ Error sending event: {e}")

        time.sleep(0.2)

    print("🏁 Done generating traffic!")


if __name__ == "__main__":
    run_load_test()