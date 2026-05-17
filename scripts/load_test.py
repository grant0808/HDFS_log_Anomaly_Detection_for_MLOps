from locust import HttpUser, between, task


class PredictUser(HttpUser):
    wait_time = between(0.01, 0.2)

    @task
    def predict(self) -> None:
        self.client.post(
            "/predict",
            json={"sequence": ["E001", "E002", "E003"], "actual_event": "E004", "metadata": {"sequence_id": "load"}},
        )
