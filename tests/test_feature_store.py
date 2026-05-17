from datetime import UTC, datetime

from feature_store import FeatureStore


def test_feature_store_write_read_versioning(tmp_path) -> None:
    store = FeatureStore(f"sqlite:///{tmp_path / 'features.db'}")
    store.init_schema()

    store.write_feature("seq1", "event_frequency", {"E001": 2}, "v1", datetime.now(UTC))

    assert store.read_feature("seq1", "event_frequency") == {"E001": 2}
    assert store.versioning() == ["v1"]
