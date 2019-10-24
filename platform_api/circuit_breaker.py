import time

from .typedefs import TimeFactory


class CircuitBreaker:
    def __init__(
        self,
        *,
        open_threshold: int = 3,
        open_timeout_s: float = 15.0,
        time_factory: TimeFactory = time.monotonic,
    ) -> None:
        assert open_threshold, "open_threshold must be > 0"
        self._failures = 0
        self._open_threshold = open_threshold
        self._open_time = 0.0
        self._open_timeout_s = open_timeout_s
        self._time_factory = time_factory

    @property
    def _half_closed_time(self) -> float:
        return self._open_time + self._open_timeout_s

    @property
    def is_closed(self) -> bool:
        return self._failures < self._open_threshold

    @property
    def is_open(self) -> bool:
        return not self.is_closed

    @property
    def is_half_closed(self) -> bool:
        return self.is_open and self._half_closed_time <= self._time_factory()

    def register_failure(self) -> None:
        was_closed = self.is_closed or self.is_half_closed
        self._failures += 1
        if was_closed and self.is_open:
            # opening
            self._open_time = self._time_factory()

    def register_success(self) -> None:
        self._failures = 0
