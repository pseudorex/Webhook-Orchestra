"""
Unit tests for SignatureService (HMAC-SHA256 payload signing).

No database, no network. Fully deterministic tests.
"""

import hmac
import hashlib
import json
import pytest

from app.services.signature_service import generate_signature


# ---------------------------------------------------------------------------
# Correctness — known input → known output
# ---------------------------------------------------------------------------

class TestGenerateSignatureCorrectness:

    def test_produces_valid_hex_string(self):
        sig = generate_signature({"key": "value"}, "mysecret")
        # HMAC-SHA256 hex digest is always 64 hex chars
        assert len(sig) == 64
        assert all(c in "0123456789abcdef" for c in sig)

    def test_matches_manual_hmac_computation(self):
        payload = {"event_id": 1, "event_type": "order.created", "payload": {"amount": 100}}
        secret = "test-secret-key"

        expected = hmac.new(
            secret.encode(),
            json.dumps(payload, sort_keys=True).encode(),
            hashlib.sha256
        ).hexdigest()

        assert generate_signature(payload, secret) == expected

    def test_deterministic_for_same_inputs(self):
        """Same payload + secret → identical signature every call."""
        payload = {"a": 1, "b": 2}
        secret = "stable-secret"
        sig1 = generate_signature(payload, secret)
        sig2 = generate_signature(payload, secret)
        assert sig1 == sig2


# ---------------------------------------------------------------------------
# Key ordering — signatures are key-order-independent
# ---------------------------------------------------------------------------

class TestSignatureKeyOrdering:

    def test_key_order_does_not_affect_signature(self):
        """
        sort_keys=True ensures that {'a':1,'b':2} and {'b':2,'a':1}
        produce the same bytes and thus the same signature.
        """
        payload_abc = {"event_type": "payment.created", "amount": 500, "currency": "USD"}
        payload_cba = {"currency": "USD", "amount": 500, "event_type": "payment.created"}
        secret = "any-secret"

        assert generate_signature(payload_abc, secret) == generate_signature(payload_cba, secret)


# ---------------------------------------------------------------------------
# Secret sensitivity
# ---------------------------------------------------------------------------

class TestSignatureSecretSensitivity:

    def test_different_secrets_produce_different_signatures(self):
        payload = {"event": "test"}
        sig1 = generate_signature(payload, "secret-A")
        sig2 = generate_signature(payload, "secret-B")
        assert sig1 != sig2

    def test_same_secret_different_payloads_differ(self):
        secret = "shared-secret"
        sig1 = generate_signature({"amount": 100}, secret)
        sig2 = generate_signature({"amount": 200}, secret)
        assert sig1 != sig2

    def test_empty_payload_still_produces_signature(self):
        sig = generate_signature({}, "some-secret")
        assert len(sig) == 64

    def test_nested_payload_is_handled(self):
        payload = {
            "event_id": 42,
            "event_type": "order.shipped",
            "payload": {
                "order_id": "ord_123",
                "items": [{"sku": "A1", "qty": 2}]
            }
        }
        sig = generate_signature(payload, "nested-secret")
        assert len(sig) == 64


# ---------------------------------------------------------------------------
# Verification pattern (as a receiver would do it)
# ---------------------------------------------------------------------------

class TestSignatureVerification:

    def test_receiver_can_verify_signature(self):
        """
        Simulates how a webhook receiver verifies the X-Webhook-Signature header:
        re-compute the signature and compare with constant-time equality.
        """
        payload = {"event_id": 7, "event_type": "invoice.paid", "payload": {"total": 999}}
        secret = "receiver-secret"

        outbound_signature = generate_signature(payload, secret)

        # Receiver recomputes:
        recomputed = hmac.new(
            secret.encode(),
            json.dumps(payload, sort_keys=True).encode(),
            hashlib.sha256
        ).hexdigest()

        assert hmac.compare_digest(outbound_signature, recomputed)

    def test_tampered_payload_fails_verification(self):
        secret = "receiver-secret"
        original_payload = {"event_id": 7, "amount": 100}
        tampered_payload = {"event_id": 7, "amount": 999}  # tampered!

        original_sig = generate_signature(original_payload, secret)

        recomputed = hmac.new(
            secret.encode(),
            json.dumps(tampered_payload, sort_keys=True).encode(),
            hashlib.sha256
        ).hexdigest()

        assert not hmac.compare_digest(original_sig, recomputed)
