from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="hdfs_deeplog_training",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["hdfs", "deeplog", "mlops"],
)
def hdfs_training_pipeline() -> None:
    @task
    def collect_logs() -> str:
        return "gs://{{ var.value.gcs_raw_bucket }}/hdfs/*.log"

    @task
    def prepare_dataset(raw_uri: str) -> str:
        return raw_uri.replace("raw", "processed").replace(".log", ".parquet")

    @task
    def train(dataset_uri: str) -> dict[str, str]:
        return {"model_uri": "artifacts/model.pt", "dataset_uri": dataset_uri}

    @task
    def evaluate(train_output: dict[str, str]) -> dict[str, object]:
        return {"model_uri": train_output["model_uri"], "accuracy": 0.95, "top_k": 3}

    @task
    def register_model(metrics: dict[str, object]) -> str:
        return f"mlflow://models/hdfs-deeplog/{metrics['top_k']}"

    @task
    def deploy(model_ref: str) -> str:
        return f"deployed:{model_ref}"

    deploy(register_model(evaluate(train(prepare_dataset(collect_logs())))))


hdfs_training_pipeline()
