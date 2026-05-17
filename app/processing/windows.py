from collections import defaultdict, deque
from dataclasses import dataclass

from app.schemas import ParsedLog


@dataclass(frozen=True)
class EventWindow:
    sequence_id: str
    host: str
    events: list[str]
    actual_event: str


class SlidingWindowBuilder:
    def __init__(self, size: int = 10) -> None:
        self.size = size
        self._buffers: dict[str, deque[str]] = defaultdict(lambda: deque(maxlen=size))

    def add(self, parsed: ParsedLog) -> EventWindow | None:
        buffer = self._buffers[parsed.sequence_id]
        if len(buffer) < self.size:
            buffer.append(parsed.event_id)
            return None
        events = list(buffer)
        window = EventWindow(
            sequence_id=parsed.sequence_id,
            host=parsed.host,
            events=events,
            actual_event=parsed.event_id,
        )
        buffer.append(parsed.event_id)
        return window
