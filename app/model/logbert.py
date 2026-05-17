class LogBERTModel:
    """Extension point for transformer-based log anomaly detection."""

    available = False

    def predict_next(self, sequence: list[str]) -> tuple[list[str], dict[str, float]]:
        raise NotImplementedError("LogBERT is optional and not enabled in this ML-first baseline")
