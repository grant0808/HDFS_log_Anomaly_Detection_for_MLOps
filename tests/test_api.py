from fastapi.testclient import TestClient

from app.main import app


def test_health() -> None:
    client = TestClient(app)
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_predict_endpoint() -> None:
    client = TestClient(app)
    response = client.post(
        "/predict",
        json={"sequence": ["E001", "E002"], "actual_event": "E003", "metadata": {"sequence_id": "test"}},
    )

    assert response.status_code == 200
    assert "top_k_predictions" in response.json()
