import pandas as pd

from monitoring.evidently import DriftService


def test_drift_detects_unseen_templates() -> None:
    service = DriftService()
    report = service.compute(
        pd.DataFrame({"template_id": ["T001"], "anomaly": [0], "confidence": [0.9]}),
        pd.DataFrame({"template_id": ["T999"], "anomaly": [1], "confidence": [0.2]}),
    )

    assert report.unseen_template_ratio == 1.0
    assert report.anomaly_count == 1
    assert report.data_drift_score > 0
