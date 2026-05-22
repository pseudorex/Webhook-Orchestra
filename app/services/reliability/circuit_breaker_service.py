from datetime import datetime, timedelta

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
                datetime.utcnow() - circuit.opened_at
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
    # RECORD SUCCESS
    # ================================

    @staticmethod
    def record_success(db, endpoint_url, tenant_id):

        circuit = CircuitBreakerService.get_or_create(
            db=db,
            endpoint_url=endpoint_url,
            tenant_id=tenant_id,
        )

        previous_state = circuit.state

        circuit.state = "closed"
        circuit.failure_count = 0
        circuit.last_success_at = datetime.utcnow()
        circuit.opened_at = None

        db.commit()

        if previous_state != "closed":

            print("=================================")
            print(
                f"CIRCUIT CLOSED: "
                f"{endpoint_url}"
            )
            print(
                f"PREVIOUS STATE: "
                f"{previous_state}"
            )
            print("=================================")

    # ================================
    # RECORD FAILURE
    # ================================

    @staticmethod
    def record_failure(db, endpoint_url, tenant_id):

        circuit = CircuitBreakerService.get_or_create(
            db=db,
            endpoint_url=endpoint_url,
            tenant_id=tenant_id,
        )

        # ----------------------------
        # CONCURRENCY CHECK
        # If the circuit is already open, ignore late-finishing
        # concurrent failures to prevent resetting the cooldown.
        # ----------------------------
        if circuit.state == "open":
            return

        circuit.failure_count += 1
        circuit.last_failure_at = datetime.utcnow()

        # ----------------------------
        # HALF_OPEN failure → re-open
        # ----------------------------

        if circuit.state == "half_open":

            circuit.state = "open"
            circuit.opened_at = datetime.utcnow()

            db.commit()

            print("=================================")
            print(
                f"CIRCUIT RE-OPENED: "
                f"{endpoint_url}"
            )
            print(
                f"HALF-OPEN TEST FAILED"
            )
            print("=================================")

            return

        # ----------------------------
        # CLOSED → check threshold
        # ----------------------------

        if circuit.failure_count >= circuit.failure_threshold:

            circuit.state = "open"
            circuit.opened_at = datetime.utcnow()

            db.commit()

            print("=================================")
            print(
                f"CIRCUIT OPENED: "
                f"{endpoint_url}"
            )
            print(
                f"FAILURES: "
                f"{circuit.failure_count}/"
                f"{circuit.failure_threshold}"
            )
            print(
                f"COOLDOWN: "
                f"{circuit.cooldown_seconds}s"
            )
            print("=================================")

            return

        db.commit()

        print("=================================")
        print(
            f"FAILURE RECORDED: "
            f"{endpoint_url}"
        )
        print(
            f"COUNT: "
            f"{circuit.failure_count}/"
            f"{circuit.failure_threshold}"
        )
        print("=================================")