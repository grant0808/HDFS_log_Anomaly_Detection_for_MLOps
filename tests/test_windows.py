from datetime import UTC, datetime

from app.processing import SlidingWindowBuilder
from app.schemas import ParsedLog


def parsed(event_id: str) -> ParsedLog:
    return ParsedLog(
        raw=f"raw {event_id}",
        template="template <*>",
        template_id="T001",
        event_id=event_id,
        timestamp=datetime.now(UTC),
        host="dn1",
        block_id="blk_1",
        sequence_id="seq",
    )


def test_sliding_window_emits_after_buffer_is_full() -> None:
    builder = SlidingWindowBuilder(size=3)
    assert builder.add(parsed("E001")) is None
    assert builder.add(parsed("E002")) is None
    assert builder.add(parsed("E003")) is None

    window = builder.add(parsed("E004"))

    assert window is not None
    assert window.events == ["E001", "E002", "E003"]
    assert window.actual_event == "E004"
