from app.model.rules import RuleDetector


def test_rule_detector_predicts_observed_transition() -> None:
    detector = RuleDetector(top_k=2)
    detector.observe(["E001", "E002", "E003"], "E004")

    top_k, probabilities = detector.predict_next(["E003"])

    assert top_k[0] == "E004"
    assert probabilities["E004"] > 0


def test_rule_detector_flags_error_like_metadata() -> None:
    assert RuleDetector.is_error_like("E100", {"message": "write failed for block"})
