from pathlib import Path

from app.config import Settings
from app.model import InferenceService
from app.workers.inference_worker import build_predict_request, process_feature_message


class FakeRepository:
    def __init__(self) -> None:
        self.rows: list[dict[str, object]] = []

    def save_inference(self, payload: dict[str, object]) -> None:
        self.rows.append(payload)


def test_build_predict_request_from_feature_window() -> None:
    request = build_predict_request(
        {
            "parsed": {"raw": "blk_1 failed", "template_id": "T001"},
            "window": {
                "sequence_id": "blk_1",
                "host": "dn1",
                "events": ["E001", "E002"],
                "actual_event": "ERR_DISK",
            },
            "features": {"window_size": 2},
        }
    )

    assert request is not None
    assert request.sequence == ["E001", "E002"]
    assert request.actual_event == "ERR_DISK"
    assert request.metadata["sequence_id"] == "blk_1"
    assert request.metadata["template_id"] == "T001"


def test_process_feature_message_writes_history_and_returns_anomaly(tmp_path) -> None:
    settings = Settings(
        model_path=Path(tmp_path / "missing.pt"),
        vocab_path=Path(tmp_path / "missing.json"),
        model_version="test",
    )
    repo = FakeRepository()
    result = process_feature_message(
        {
            "window": {
                "sequence_id": "blk_1",
                "host": "dn1",
                "events": ["E001", "E002"],
                "actual_event": "ERR_DISK",
            }
        },
        InferenceService(settings),
        repo,
    )

    assert result is not None
    assert result["anomaly"] is True
    assert result["model_version"] == "test:rules"
    assert len(repo.rows) == 1
