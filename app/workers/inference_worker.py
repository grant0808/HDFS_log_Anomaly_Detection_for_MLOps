from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, Protocol

from app.config import Settings, get_settings
from app.db import MetadataRepository
from app.db.clickhouse import ClickHouseHistoryWriter
from app.model import InferenceService
from app.schemas import PredictRequest, PredictResponse


class InferenceRepository(Protocol):
    def save_inference(self, payload: dict[str, Any]) -> None:
        ...


def build_predict_request(message: dict[str, Any]) -> PredictRequest | None:
    window = message.get("window")
    if not isinstance(window, dict):
        return None
    events = window.get("events")
    if not isinstance(events, list) or not all(isinstance(event, str) for event in events):
        return None
    metadata = {
        "sequence_id": window.get("sequence_id"),
        "host": window.get("host"),
        "features": message.get("features"),
    }
    parsed = message.get("parsed")
    if isinstance(parsed, dict):
        metadata["message"] = parsed.get("raw", "")
        metadata["template_id"] = parsed.get("template_id")
    return PredictRequest(
        sequence=events,
        actual_event=window.get("actual_event"),
        metadata={key: value for key, value in metadata.items() if value is not None},
    )


def build_inference_row(request: PredictRequest, response: PredictResponse) -> dict[str, Any]:
    return {
        "timestamp": datetime.now(UTC),
        "sequence_id": request.metadata.get("sequence_id"),
        "actual_event": request.actual_event,
        "anomaly": int(response.anomaly),
        "confidence": response.confidence,
        "latency_ms": response.latency_ms,
        "model_version": response.model_version,
        "payload": response.model_dump(),
    }


def process_feature_message(
    message: dict[str, Any],
    inference: InferenceService,
    repo: InferenceRepository,
    clickhouse: ClickHouseHistoryWriter | None = None,
) -> dict[str, Any] | None:
    request = build_predict_request(message)
    if request is None:
        return None
    response = inference.predict(request)
    row = build_inference_row(request, response)
    repo.save_inference({key: value for key, value in row.items() if key != "model_version"})
    if clickhouse is not None:
        clickhouse.write_inference([row])
    return {
        "timestamp": row["timestamp"].isoformat(),
        "sequence_id": row["sequence_id"],
        "actual_event": row["actual_event"],
        "anomaly": bool(row["anomaly"]),
        "confidence": row["confidence"],
        "latency_ms": row["latency_ms"],
        "model_version": row["model_version"],
        "response": response.model_dump(),
    }


class KafkaInferenceWorker:
    def __init__(self, settings: Settings) -> None:
        try:
            from confluent_kafka import Consumer, Producer
        except ImportError as exc:
            raise RuntimeError("Install project runtime dependencies to run the inference worker") from exc

        self.settings = settings
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "group.id": "hdfs-inference-worker",
                "auto.offset.reset": "latest",
                "enable.auto.commit": False,
            }
        )
        self.producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
        self.inference = InferenceService(settings)
        self.repo = MetadataRepository(settings.postgres_dsn)
        self.clickhouse: ClickHouseHistoryWriter | None = None
        if settings.clickhouse_enabled:
            self.clickhouse = ClickHouseHistoryWriter(settings)
            self.clickhouse.init_schema()

    def run(self) -> None:
        self.consumer.subscribe([self.settings.feature_topic])
        try:
            while True:
                message = self.consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    raise RuntimeError(str(message.error()))
                payload = json.loads(message.value().decode("utf-8"))
                result = process_feature_message(payload, self.inference, self.repo, self.clickhouse)
                if result is None:
                    self.consumer.commit(message)
                    continue
                encoded = json.dumps(result, default=str).encode("utf-8")
                self.producer.produce(self.settings.anomaly_topic, encoded)
                self.producer.produce(self.settings.inference_topic, encoded)
                self.producer.poll(0)
                self.consumer.commit(message)
        finally:
            self.producer.flush()
            self.consumer.close()


def main() -> None:
    KafkaInferenceWorker(get_settings()).run()


if __name__ == "__main__":
    main()
