from app.services.reliability.failure_classifier import (
    FailureType
)

from app.services.reliability.backoff import Backoff


class RetryPolicy:

    @staticmethod
    def get_delay(failure_type, attempt):

        if failure_type == FailureType.TIMEOUT:
            return Backoff.timeout_backoff(attempt)

        if failure_type == FailureType.DNS_ERROR:
            return Backoff.dns_backoff(attempt)

        if failure_type == FailureType.RATE_LIMITED:
            return Backoff.rate_limit_backoff(attempt)

        return Backoff.exponential(attempt)

    @staticmethod
    def max_retries(failure_type):

        if failure_type == FailureType.PERMANENT:
            return 0

        if failure_type == FailureType.RATE_LIMITED:
            return 10

        return 5