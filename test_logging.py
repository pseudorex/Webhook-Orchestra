import json
import logging
import io
import sys
from app.core.logging import (
    setup_app_logging,
    correlation_id_var,
    tenant_id_var,
    event_id_var,
    StructuredJsonFormatter
)


def run_test():
    # Capture stdout
    stdout_buf = io.StringIO()

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Setup handler writing to buffer
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    handler = logging.StreamHandler(stdout_buf)
    handler.setFormatter(StructuredJsonFormatter())
    root_logger.addHandler(handler)

    # Test 1: Basic log
    logging.info("Test basic message")

    # Test 2: Log with context variables
    corr_token = correlation_id_var.set("test-corr-id-123")
    tenant_token = tenant_id_var.set(42)
    event_token = event_id_var.set(999)

    logging.warning("Test message with context")

    # Reset context
    correlation_id_var.reset(corr_token)
    tenant_id_var.reset(tenant_token)
    event_id_var.reset(event_token)

    # Test 3: Log after reset (should not have context)
    logging.info("Test message after reset")

    # Read and parse logs
    logs = stdout_buf.getvalue().strip().split("\n")
    print(f"Captured {len(logs)} logs.")

    # Parse Log 1
    log1 = json.loads(logs[0])
    assert log1["message"] == "Test basic message"
    assert "correlation_id" not in log1
    assert "tenant_id" not in log1
    assert "event_id" not in log1
    print("Log 1 verified successfully.")

    # Parse Log 2
    log2 = json.loads(logs[1])
    assert log2["message"] == "Test message with context"
    assert log2["correlation_id"] == "test-corr-id-123"
    assert log2["tenant_id"] == 42
    assert log2["event_id"] == 999
    assert log2["level"] == "WARNING"
    print("Log 2 verified successfully (correlation_id, tenant_id, and event_id injected).")

    # Parse Log 3
    log3 = json.loads(logs[2])
    assert log3["message"] == "Test message after reset"
    assert "correlation_id" not in log3
    print("Log 3 verified successfully (context cleaned up).")

    print("\nAll logging unit tests passed successfully!")


if __name__ == "__main__":
    run_test()