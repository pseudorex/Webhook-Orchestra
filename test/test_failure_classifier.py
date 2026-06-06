"""
Unit tests for FailureClassifier.

These are pure-logic tests — no database, no network, no Celery.
They verify that every HTTP status code and exception type is mapped
to the correct FailureType, and that the retryability flag is correct.
"""

import pytest
import requests.exceptions

from app.services.reliability.failure_classifier import FailureClassifier, FailureType


# ---------------------------------------------------------------------------
# classify() — HTTP status codes
# ---------------------------------------------------------------------------

class TestClassifyByStatusCode:
    """classify() called with a status_code argument."""

    # --- Rate limiting -------------------------------------------------
    def test_429_is_rate_limited(self):
        assert FailureClassifier.classify(status_code=429) == FailureType.RATE_LIMITED

    # --- Server-side transient errors ---------------------------------
    def test_500_is_transient(self):
        assert FailureClassifier.classify(status_code=500) == FailureType.TRANSIENT

    def test_502_is_transient(self):
        assert FailureClassifier.classify(status_code=502) == FailureType.TRANSIENT

    def test_503_is_transient(self):
        assert FailureClassifier.classify(status_code=503) == FailureType.TRANSIENT

    def test_504_is_transient(self):
        assert FailureClassifier.classify(status_code=504) == FailureType.TRANSIENT

    # --- Permanent client-side errors ---------------------------------
    def test_400_is_permanent(self):
        assert FailureClassifier.classify(status_code=400) == FailureType.PERMANENT

    def test_401_is_permanent(self):
        assert FailureClassifier.classify(status_code=401) == FailureType.PERMANENT

    def test_403_is_permanent(self):
        assert FailureClassifier.classify(status_code=403) == FailureType.PERMANENT

    def test_404_is_permanent(self):
        assert FailureClassifier.classify(status_code=404) == FailureType.PERMANENT

    # --- Unknown / unmapped codes fall back to TRANSIENT --------------
    def test_unknown_4xx_falls_back_to_transient(self):
        # e.g. 422 Unprocessable Entity — not in the explicit list
        result = FailureClassifier.classify(status_code=422)
        assert result == FailureType.TRANSIENT

    def test_unknown_5xx_is_transient(self):
        assert FailureClassifier.classify(status_code=599) == FailureType.TRANSIENT


# ---------------------------------------------------------------------------
# classify() — exception types
# ---------------------------------------------------------------------------

class TestClassifyByException:
    """classify() called with an error argument."""

    def test_timeout_exception(self):
        # The classifier checks str(error).lower() for 'timeout'.
        # requests.exceptions.Timeout str() is often empty, so we test with
        # a plain Exception that mirrors what a real socket timeout produces.
        err = Exception("HTTPConnectionPool: Read timed out. (read timeout=10)")
        assert FailureClassifier.classify(error=err) == FailureType.TIMEOUT

    def test_connection_timeout_exception(self):
        err = Exception("Connection timeout: timed out connecting to host")
        assert FailureClassifier.classify(error=err) == FailureType.TIMEOUT

    def test_connection_refused_exception(self):
        err = ConnectionRefusedError("Connection refused")
        assert FailureClassifier.classify(error=err) == FailureType.CONNECTION_REFUSED

    def test_dns_error_via_string_match(self):
        # Simulate a socket.gaierror whose str() contains the known DNS string
        err = Exception("name or service not known")
        assert FailureClassifier.classify(error=err) == FailureType.DNS_ERROR

    def test_generic_exception_falls_back_to_transient(self):
        err = Exception("something unexpected happened")
        assert FailureClassifier.classify(error=err) == FailureType.TRANSIENT

    def test_runtime_error_falls_back_to_transient(self):
        err = RuntimeError("internal error")
        assert FailureClassifier.classify(error=err) == FailureType.TRANSIENT


# ---------------------------------------------------------------------------
# is_retryable()
# ---------------------------------------------------------------------------

class TestIsRetryable:
    """is_retryable() should return False only for PERMANENT failures."""

    def test_transient_is_retryable(self):
        assert FailureClassifier.is_retryable(FailureType.TRANSIENT) is True

    def test_timeout_is_retryable(self):
        assert FailureClassifier.is_retryable(FailureType.TIMEOUT) is True

    def test_rate_limited_is_retryable(self):
        assert FailureClassifier.is_retryable(FailureType.RATE_LIMITED) is True

    def test_dns_error_is_retryable(self):
        assert FailureClassifier.is_retryable(FailureType.DNS_ERROR) is True

    def test_connection_refused_is_retryable(self):
        assert FailureClassifier.is_retryable(FailureType.CONNECTION_REFUSED) is True

    def test_permanent_is_not_retryable(self):
        assert FailureClassifier.is_retryable(FailureType.PERMANENT) is False


# ---------------------------------------------------------------------------
# FailureType enum sanity
# ---------------------------------------------------------------------------

class TestFailureTypeEnum:
    """Ensure the enum values are strings (used as DB column values)."""

    def test_enum_values_are_strings(self):
        for member in FailureType:
            assert isinstance(member.value, str), f"{member} value should be a string"

    def test_all_expected_types_exist(self):
        expected = {
            "TRANSIENT", "PERMANENT", "TIMEOUT",
            "RATE_LIMITED", "DNS_ERROR", "CONNECTION_REFUSED"
        }
        actual = {m.value for m in FailureType}
        assert expected == actual
