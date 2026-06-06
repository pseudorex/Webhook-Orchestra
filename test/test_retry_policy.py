"""
Unit tests for RetryPolicy and Backoff strategies.

No database, no network. These are pure numerical / dispatch tests.
"""

import pytest

from app.services.reliability.failure_classifier import FailureType
from app.services.reliability.retry_policy import RetryPolicy
from app.services.reliability.backoff import Backoff


# ---------------------------------------------------------------------------
# Backoff — deterministic strategies
# ---------------------------------------------------------------------------

class TestBackoffDeterministic:
    """Test the non-random (linear) backoff strategies."""

    # Timeout: 2 * attempt
    def test_timeout_backoff_attempt_1(self):
        assert Backoff.timeout_backoff(1) == 2

    def test_timeout_backoff_attempt_3(self):
        assert Backoff.timeout_backoff(3) == 6

    def test_timeout_backoff_attempt_5(self):
        assert Backoff.timeout_backoff(5) == 10

    # DNS: 5 * attempt
    def test_dns_backoff_attempt_1(self):
        assert Backoff.dns_backoff(1) == 5

    def test_dns_backoff_attempt_5(self):
        assert Backoff.dns_backoff(5) == 25

    # Rate-limit: 60 * attempt
    def test_rate_limit_backoff_attempt_1(self):
        assert Backoff.rate_limit_backoff(1) == 60

    def test_rate_limit_backoff_attempt_2(self):
        assert Backoff.rate_limit_backoff(2) == 120

    def test_rate_limit_backoff_attempt_10(self):
        assert Backoff.rate_limit_backoff(10) == 600


class TestBackoffExponential:
    """Exponential backoff has jitter so we test bounds, not exact values."""

    def test_exponential_attempt_1_lower_bound(self):
        # 2^1 + jitter(0,1) → always >= 2.0
        result = Backoff.exponential(1)
        assert result >= 2.0

    def test_exponential_attempt_1_upper_bound(self):
        # 2^1 + jitter(0,1) → always < 3.0
        result = Backoff.exponential(1)
        assert result < 3.0

    def test_exponential_attempt_3_lower_bound(self):
        # 2^3 + jitter → always >= 8.0
        result = Backoff.exponential(3)
        assert result >= 8.0

    def test_exponential_attempt_3_upper_bound(self):
        # 2^3 + jitter(0,1) → always < 9.0
        result = Backoff.exponential(3)
        assert result < 9.0

    def test_exponential_attempt_5_lower_bound(self):
        # 2^5 = 32 + jitter → always >= 32.0
        result = Backoff.exponential(5)
        assert result >= 32.0

    def test_exponential_grows_with_attempt(self):
        """Each subsequent attempt should generally produce a higher base delay."""
        delays = [Backoff.exponential(i) - 1 for i in range(1, 6)]  # subtract max jitter
        for i in range(len(delays) - 1):
            assert delays[i] < delays[i + 1], (
                f"Delay at attempt {i+1} should be less than at attempt {i+2}"
            )


# ---------------------------------------------------------------------------
# RetryPolicy.max_retries()
# ---------------------------------------------------------------------------

class TestMaxRetries:
    """Verify max retry limits per failure type."""

    def test_permanent_max_retries_is_zero(self):
        assert RetryPolicy.max_retries(FailureType.PERMANENT) == 0

    def test_rate_limited_max_retries_is_ten(self):
        assert RetryPolicy.max_retries(FailureType.RATE_LIMITED) == 10

    def test_transient_max_retries_is_five(self):
        assert RetryPolicy.max_retries(FailureType.TRANSIENT) == 5

    def test_timeout_max_retries_is_five(self):
        assert RetryPolicy.max_retries(FailureType.TIMEOUT) == 5

    def test_dns_error_max_retries_is_five(self):
        assert RetryPolicy.max_retries(FailureType.DNS_ERROR) == 5

    def test_connection_refused_max_retries_is_five(self):
        assert RetryPolicy.max_retries(FailureType.CONNECTION_REFUSED) == 5


# ---------------------------------------------------------------------------
# RetryPolicy.get_delay() — dispatches to the right Backoff strategy
# ---------------------------------------------------------------------------

class TestGetDelayDispatch:
    """get_delay() must route each failure type to its correct backoff."""

    def test_timeout_dispatches_to_timeout_backoff(self):
        attempt = 3
        expected = Backoff.timeout_backoff(attempt)
        assert RetryPolicy.get_delay(FailureType.TIMEOUT, attempt) == expected

    def test_dns_error_dispatches_to_dns_backoff(self):
        attempt = 2
        expected = Backoff.dns_backoff(attempt)
        assert RetryPolicy.get_delay(FailureType.DNS_ERROR, attempt) == expected

    def test_rate_limited_dispatches_to_rate_limit_backoff(self):
        attempt = 4
        expected = Backoff.rate_limit_backoff(attempt)
        assert RetryPolicy.get_delay(FailureType.RATE_LIMITED, attempt) == expected

    def test_transient_dispatches_to_exponential(self):
        """TRANSIENT should fall through to exponential backoff."""
        attempt = 2
        result = RetryPolicy.get_delay(FailureType.TRANSIENT, attempt)
        # 2^2 = 4, jitter adds 0-1
        assert 4.0 <= result < 5.0

    def test_connection_refused_dispatches_to_exponential(self):
        """CONNECTION_REFUSED also uses exponential (falls through)."""
        attempt = 1
        result = RetryPolicy.get_delay(FailureType.CONNECTION_REFUSED, attempt)
        assert 2.0 <= result < 3.0

    def test_delay_increases_with_attempt_for_rate_limited(self):
        delays = [RetryPolicy.get_delay(FailureType.RATE_LIMITED, i) for i in range(1, 6)]
        for i in range(len(delays) - 1):
            assert delays[i] < delays[i + 1]

    def test_delay_increases_with_attempt_for_timeout(self):
        delays = [RetryPolicy.get_delay(FailureType.TIMEOUT, i) for i in range(1, 6)]
        for i in range(len(delays) - 1):
            assert delays[i] < delays[i + 1]
