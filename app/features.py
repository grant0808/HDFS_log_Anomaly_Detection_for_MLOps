from collections import Counter

from app.processing.windows import EventWindow


def extract_window_features(window: EventWindow, latency_ms: float | None = None) -> dict[str, object]:
    counts = Counter(window.events)
    errors = sum(1 for event in window.events if event.upper().startswith("ERR"))
    return {
        "event_frequency": dict(counts),
        "window_size": len(window.events),
        "unique_templates": len(counts),
        "host": window.host,
        "template_counts": dict(counts),
        "host_counts": {window.host: len(window.events)},
        "latency_ms": latency_ms or 0.0,
        "error_ratio": errors / max(len(window.events), 1),
        "sequence_embedding": [counts[event] / len(window.events) for event in sorted(counts)],
    }
