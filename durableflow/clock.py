"""Time, as the engine sees it. Do not modify."""


class Clock:
    def now(self) -> float:
        raise NotImplementedError


class FakeClock(Clock):
    """A clock that moves only when told to."""

    def __init__(self, start: float = 0.0):
        self._t = float(start)

    def now(self) -> float:
        return self._t

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("a clock does not go back")
        self._t += seconds

    def set(self, t: float) -> None:
        if t < self._t:
            raise ValueError("a clock does not go back")
        self._t = float(t)
