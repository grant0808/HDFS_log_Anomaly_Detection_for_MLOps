from app.features import extract_window_features
from app.processing.windows import EventWindow


def test_extract_window_features() -> None:
    features = extract_window_features(
        EventWindow(sequence_id="seq", host="dn1", events=["E001", "ERR_DISK", "E001"], actual_event="E002")
    )

    assert features["event_frequency"] == {"E001": 2, "ERR_DISK": 1}
    assert features["error_ratio"] == 1 / 3
    assert features["host_counts"] == {"dn1": 3}
