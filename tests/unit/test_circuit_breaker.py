from dataclasses import dataclass

from platform_api.circuit_breaker import CircuitBreaker


@dataclass
class _TestTimeFactory:
    time: float = 0

    def __call__(self) -> float:
        return self.time


class TestCircuitBreaker:
    def test_idle_closed(self) -> None:
        breaker = CircuitBreaker()
        assert breaker.is_closed
        assert not breaker.is_half_closed
        assert not breaker.is_open

    def test_threashold_not_exceeded(self) -> None:
        breaker = CircuitBreaker(open_threshold=2)
        breaker.register_failure()
        assert breaker.is_closed
        assert not breaker.is_half_closed
        assert not breaker.is_open

    def test_threashold_exceeded(self) -> None:
        breaker = CircuitBreaker(open_threshold=1)
        breaker.register_failure()
        assert not breaker.is_closed
        assert not breaker.is_half_closed
        assert breaker.is_open

    def test_closed_after_success(self) -> None:
        breaker = CircuitBreaker(open_threshold=1)

        breaker.register_failure()
        assert not breaker.is_closed
        assert not breaker.is_half_closed
        assert breaker.is_open

        breaker.register_success()
        assert breaker.is_closed
        assert not breaker.is_half_closed
        assert not breaker.is_open

    def test_half_closed_success(self) -> None:
        time_factory = _TestTimeFactory(time=1)
        breaker = CircuitBreaker(
            open_threshold=1, open_timeout_s=1, time_factory=time_factory
        )

        breaker.register_failure()
        assert not breaker.is_closed
        assert not breaker.is_half_closed
        assert breaker.is_open

        # half closing
        time_factory.time = 2

        assert not breaker.is_closed
        assert breaker.is_half_closed
        assert breaker.is_open

        breaker.register_success()
        assert breaker.is_closed
        assert not breaker.is_half_closed
        assert not breaker.is_open

    def test_half_closed_failure(self) -> None:
        time_factory = _TestTimeFactory(time=1)
        breaker = CircuitBreaker(
            open_threshold=1, open_timeout_s=1, time_factory=time_factory
        )

        breaker.register_failure()
        assert not breaker.is_closed
        assert not breaker.is_half_closed
        assert breaker.is_open

        # half closing
        time_factory.time = 2

        assert not breaker.is_closed
        assert breaker.is_half_closed
        assert breaker.is_open

        breaker.register_failure()
        assert not breaker.is_closed
        assert not breaker.is_half_closed
        assert breaker.is_open

        # half closing
        time_factory.time = 3

        assert not breaker.is_closed
        assert breaker.is_half_closed
        assert breaker.is_open

        breaker.register_success()
        assert breaker.is_closed
        assert not breaker.is_half_closed
        assert not breaker.is_open
