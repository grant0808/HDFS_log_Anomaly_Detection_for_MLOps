from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    service_name: str = "hdfs-log-anomaly-api"
    environment: str = "local"
    log_level: str = "INFO"

    postgres_dsn: str = "sqlite:///./metadata.db"
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "hdfs_observability"
    redis_url: str = "redis://localhost:6379/0"

    kafka_bootstrap_servers: str = "localhost:9092"
    raw_topic: str = "hdfs.raw.logs"
    parsed_topic: str = "hdfs.parsed.events"
    feature_topic: str = "hdfs.feature.windows"
    anomaly_topic: str = "hdfs.anomalies"
    inference_topic: str = "hdfs.inference.history"

    model_path: Path = Path("artifacts/model.pt")
    vocab_path: Path = Path("artifacts/vocab.json")
    top_k: int = 3
    sequence_length: int = 10
    anomaly_threshold: float = 0.5
    model_version: str = "local-rule-fallback"

    smtp_host: str = "localhost"
    smtp_port: int = 1025
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_from: str = "hdfs-alerts@example.com"
    smtp_to: str = "mlops@example.com"
    alert_drift_threshold: float = 0.35
    alert_anomaly_spike_threshold: int = 25
    alert_latency_ms_threshold: float = 250.0
    alert_consumer_lag_threshold: int = 1000

    gcp_project_id: str = "local-dev"
    gcs_raw_bucket: str = "hdfs-raw-logs"
    gcs_processed_bucket: str = "hdfs-processed-logs"
    gcs_artifacts_bucket: str = "hdfs-model-artifacts"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
