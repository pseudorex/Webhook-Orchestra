from enum import Enum


class FailureType(str, Enum):

    TRANSIENT = "TRANSIENT"

    PERMANENT = "PERMANENT"

    TIMEOUT = "TIMEOUT"

    RATE_LIMITED = "RATE_LIMITED"

    DNS_ERROR = "DNS_ERROR"

    CONNECTION_REFUSED = "CONNECTION_REFUSED"


class FailureClassifier:

    @staticmethod
    def classify(error=None, status_code=None):

        if status_code == 429:
            return FailureType.RATE_LIMITED

        if status_code and status_code >= 500:
            return FailureType.TRANSIENT

        if status_code in [400, 401, 403, 404]:
            return FailureType.PERMANENT

        if error:

            error_text = str(error).lower()

            if "timeout" in error_text:
                return FailureType.TIMEOUT

            if "name or service not known" in error_text:
                return FailureType.DNS_ERROR

            if "connection refused" in error_text:
                return FailureType.CONNECTION_REFUSED

        return FailureType.TRANSIENT

    @staticmethod
    def is_retryable(failure_type):

        return failure_type != FailureType.PERMANENT