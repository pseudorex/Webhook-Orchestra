import random


class Backoff:

    @staticmethod
    def exponential(attempt: int):

        base_delay = 2

        exponential = base_delay ** attempt

        jitter = random.uniform(0, 1)

        return exponential + jitter

    @staticmethod
    def timeout_backoff(attempt: int):

        return 2 * attempt

    @staticmethod
    def dns_backoff(attempt: int):

        return 5 * attempt

    @staticmethod
    def rate_limit_backoff(attempt: int):

        return 60 * attempt