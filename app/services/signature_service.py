import hmac
import hashlib
import json


def generate_signature(payload: dict, secret: str) -> str:

    payload_bytes = json.dumps(
        payload,
        sort_keys=True
    ).encode()

    return hmac.new(
        secret.encode(),
        payload_bytes,
        hashlib.sha256
    ).hexdigest()