from pathlib import Path

from app.config import Settings
from app.model import InferenceService
from app.schemas import PredictRequest


def test_inference_uses_rule_fallback_without_model_artifact(tmp_path) -> None:
    settings = Settings(
        model_path=Path(tmp_path / "missing.pt"),
        vocab_path=Path(tmp_path / "missing.json"),
        model_version="test",
    )
    service = InferenceService(settings)

    response = service.predict(PredictRequest(sequence=["E001", "E002"], actual_event="E003"))

    assert response.rule_fallback is True
    assert response.model_version == "test:rules"
    assert response.top_k_predictions
