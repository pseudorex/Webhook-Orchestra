from datetime import datetime, timedelta, timezone

from app.models.circuit_breaker import CircuitBreaker


class CircuitBreakerService:

    # ================================
    # GET OR CREATE CIRCUIT
    # ================================

    @staticmethod
    def get_or_create(db, endpoint_url, tenant_id):

        circuit = (
            db.query(CircuitBreaker)
            .filter(
                CircuitBreaker.endpoint_url == endpoint_url
            )
            .first()
        )

        if not circuit:
            circuit = CircuitBreaker(
                endpoint_url=endpoint_url,
                tenant_id=tenant_id,
                state="closed",
                failure_count=0,
            )

            db.add(circuit)
            db.commit()
            db.refresh(circuit)

        return circuit

    # ================================
    # CHECK IF REQUEST IS ALLOWED
    # ================================

    @staticmethod
    def should_allow_request(db, endpoint_url, tenant_id):

        circuit = CircuitBreakerService.get_or_create(
            db=db,
            endpoint_url=endpoint_url,
            tenant_id=tenant_id,
        )

        # ----------------------------
        # CLOSED → allow all
        # ----------------------------

        if circuit.state == "closed":
            return True

        # ----------------------------
        # OPEN → check cooldown
        # ----------------------------

        if circuit.state == "open":

            if circuit.opened_at is None:
                return False

            elapsed = (
                    datetime.now(timezone.utc) - circuit.opened_at
            )

            cooldown = timedelta(
                seconds=circuit.cooldown_seconds
            )

            # Cooldown expired → move to half_open
            if elapsed >= cooldown:
                circuit.state = "half_open"
                db.commit()

                print("=================================")
                print(
                    f"CIRCUIT HALF-OPEN: "
                    f"{endpoint_url}"
                )
                print("=================================")

                return True

            # Still in cooldown → block
            print("=================================")
            print(
                f"CIRCUIT OPEN — BLOCKING: "
                f"{endpoint_url}"
            )
            print(
                f"RETRY AFTER: "
                f"{cooldown - elapsed}"
            )
            print("=================================")

            return False

        # ----------------------------
        # HALF_OPEN → allow one test
        # ----------------------------

        if circuit.state == "half_open":
            return True

        return False

    # ================================
    # CALCULATE HEALTH (Phase 5.3)
    # ================================

    @staticmethod
    def calculate_health(circuit):
        """
        Calculates the health score (0.0 to 100.0) and transitions the health state:
        - Consecutive failures: Deducts 20 points per failure.
        - Latency penalty: Deducts points if average latency > 200ms.
        - Success rate penalty: Deducts points based on drop in success rate.
        - Open Circuit: Forces score to 0.0.
        """
        if circuit.state == "open":
            circuit.health_score = 0.0
            circuit.health_state = "unhealthy"
            return

        score = 100.0

        # 1. Consecutive Failures Penalty (20 points per failure)
        score -= (circuit.consecutive_failures * 20.0)

        # 2. Latency Penalty (Deduct points if average latency exceeds 200ms)
        if circuit.average_latency_ms > 200.0:
            latency_excess = circuit.average_latency_ms - 200.0
            # Deduct 1 point per 10ms over 200ms, up to a max penalty of 50 points
            latency_penalty = min(latency_excess / 10.0, 50.0)
            score -= latency_penalty

        # 3. Success Rate Penalty
        if circuit.success_rate < 100.0:
            # Deduct half a point for every 1% drop in success rate
            score -= (100.0 - circuit.success_rate) * 0.5

        # Bound score between 0.0 and 100.0
        circuit.health_score = max(0.0, min(100.0, score))

        # Determine health state
        if circuit.health_score >= 80.0:
            circuit.health_state = "healthy"
        elif circuit.health_score >= 50.0:
            circuit.health_state = "degraded"
        else:
            circuit.health_state = "unhealthy"

    # ================================
    # RECORD SUCCESS
    # ================================

    @staticmethod
    def record_success(db, endpoint_url, tenant_id, latency_ms=None):
        circuit = CircuitBreakerService.get_or_create(
            db=db,
            endpoint_url=endpoint_url,
            tenant_id=tenant_id,
        )

        previous_state = circuit.state

        # Update metrics
        circuit.state = "closed"
        circuit.failure_count = 0
        circuit.consecutive_failures = 0
        circuit.last_success_at = datetime.now(timezone.utc)
        circuit.opened_at = None

        circuit.total_requests += 1
        circuit.success_count += 1
        circuit.success_rate = (circuit.success_count / circuit.total_requests) * 100.0

        if latency_ms is not None:
            if circuit.total_requests == 1:
                circuit.average_latency_ms = float(latency_ms)
            else:
                # Cumulative moving average
                circuit.average_latency_ms = (
                                                     (circuit.average_latency_ms * (
                                                                 circuit.total_requests - 1)) + latency_ms
                                             ) / circuit.total_requests

        # Recalculate health
        CircuitBreakerService.calculate_health(circuit)

        db.commit()

        if previous_state != "closed":
            print("=================================")
            print(f"CIRCUIT CLOSED: {endpoint_url}")
            print(f"PREVIOUS STATE: {previous_state}")
            print("=================================")

    # ================================
    # RECORD FAILURE
    # ================================

    @staticmethod
    def record_failure(db, endpoint_url, tenant_id, latency_ms=None):
        circuit = CircuitBreakerService.get_or_create(
            db=db,
            endpoint_url=endpoint_url,
            tenant_id=tenant_id,
        )

        # Concurrency check
        if circuit.state == "open":
            return

        circuit.failure_count += 1
        circuit.consecutive_failures += 1
        circuit.last_failure_at = datetime.now(timezone.utc)

        circuit.total_requests += 1
        circuit.success_rate = (circuit.success_count / circuit.total_requests) * 100.0

        if latency_ms is not None:
            if circuit.total_requests == 1:
                circuit.average_latency_ms = float(latency_ms)
            else:
                circuit.average_latency_ms = (
                                                     (circuit.average_latency_ms * (
                                                                 circuit.total_requests - 1)) + latency_ms
                                             ) / circuit.total_requests

        # HALF_OPEN failure → re-open
        if circuit.state == "half_open":
            circuit.state = "open"
            circuit.opened_at = datetime.now(timezone.utc)

            CircuitBreakerService.calculate_health(circuit)
            db.commit()

            print("=================================")
            print(f"CIRCUIT RE-OPENED: {endpoint_url}")
            print("HALF-OPEN TEST FAILED")
            print("=================================")
            return

        # CLOSED → check threshold
        if circuit.failure_count >= circuit.failure_threshold:
            circuit.state = "open"
            circuit.opened_at = datetime.now(timezone.utc)

            CircuitBreakerService.calculate_health(circuit)
            db.commit()

            print("=================================")
            print(f"CIRCUIT OPENED: {endpoint_url}")
            print(f"FAILURES: {circuit.failure_count}/{circuit.failure_threshold}")
            print(f"COOLDOWN: {circuit.cooldown_seconds}s")
            print("=================================")
            return

        # Recalculate health for transient failures before threshold
        CircuitBreakerService.calculate_health(circuit)
        db.commit()

        print("=================================")
        print(f"FAILURE RECORDED: {endpoint_url}")
        print(f"COUNT: {circuit.failure_count}/{circuit.failure_threshold}")
        print("=================================")