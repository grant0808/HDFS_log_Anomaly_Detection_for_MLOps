import time

from app.config import Settings
from app.metrics import ANOMALIES, ANOMALY_RATE, INFERENCE_LATENCY, MODEL_VERSION, PREDICTION_CONFIDENCE
from app.model.deeplog import DeepLogModel
from app.model.rules import RuleDetector
from app.schemas import PredictRequest, PredictResponse


class InferenceService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.deeplog = DeepLogModel(settings.model_path, settings.vocab_path, top_k=settings.top_k)
        self.rules = RuleDetector(top_k=settings.top_k)
        self._recent_predictions = 0
        self._recent_anomalies = 0
        MODEL_VERSION.labels(version=settings.model_version).set(1)

    def predict(self, request: PredictRequest) -> PredictResponse:
        start = time.perf_counter()
        self.rules.observe(request.sequence, request.actual_event)
        rule_fallback = False
        try:
            top_k, probabilities = self.deeplog.predict_next(request.sequence)
            model_version = self.settings.model_version
        except Exception:
            rule_fallback = True
            top_k, probabilities = self.rules.predict_next(request.sequence)
            model_version = f"{self.settings.model_version}:rules"

        actual = request.actual_event
        anomaly = bool(actual and actual not in top_k)
        if self.rules.is_error_like(actual or "", request.metadata):
            anomaly = True

        confidence = max(probabilities.values()) if probabilities else 0.0
        latency_ms = (time.perf_counter() - start) * 1000
        self._recent_predictions += 1
        if anomaly:
            self._recent_anomalies += 1
            ANOMALIES.inc()
        ANOMALY_RATE.set(self._recent_anomalies / max(self._recent_predictions, 1))
        PREDICTION_CONFIDENCE.set(confidence)
        INFERENCE_LATENCY.observe(latency_ms / 1000)
        reason = "actual_event_not_in_top_k" if anomaly else "actual_event_in_top_k_or_missing"
        if rule_fallback:
            reason = f"rule_fallback:{reason}"
        return PredictResponse(
            anomaly=anomaly,
            actual_event=actual,
            top_k_predictions=top_k,
            probabilities=probabilities,
            confidence=confidence,
            model_version=model_version,
            rule_fallback=rule_fallback,
            latency_ms=latency_ms,
            reason=reason,
        )
