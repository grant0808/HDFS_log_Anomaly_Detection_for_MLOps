from collections import Counter


class RuleDetector:
    def __init__(self, top_k: int = 3) -> None:
        self.top_k = top_k
        self._transitions: dict[str, Counter[str]] = {}
        self._global_counts: Counter[str] = Counter()

    def observe(self, sequence: list[str], actual_event: str | None = None) -> None:
        for previous, current in zip(sequence, sequence[1:], strict=False):
            self._transitions.setdefault(previous, Counter())[current] += 1
            self._global_counts[current] += 1
        if actual_event:
            self._global_counts[actual_event] += 1
            if sequence:
                self._transitions.setdefault(sequence[-1], Counter())[actual_event] += 1

    def predict_next(self, sequence: list[str]) -> tuple[list[str], dict[str, float]]:
        if not sequence:
            candidates = self._global_counts
        else:
            candidates = self._transitions.get(sequence[-1], Counter())
            if not candidates:
                candidates = self._global_counts
        if not candidates:
            defaults = [f"E{i:03d}" for i in range(1, self.top_k + 1)]
            return defaults, {event: 1.0 / len(defaults) for event in defaults}
        total = sum(candidates.values())
        pairs = candidates.most_common(self.top_k)
        return [event for event, _ in pairs], {event: count / total for event, count in pairs}

    @staticmethod
    def is_error_like(event: str, metadata: dict[str, object] | None = None) -> bool:
        message = str((metadata or {}).get("message", "")).lower()
        return event.upper().startswith("ERR") or any(token in message for token in ("error", "exception", "failed"))
