from datetime import datetime
from typing import Any

from sqlalchemy import JSON, DateTime, Float, Integer, MetaData, String, Table, create_engine, insert, select
from sqlalchemy.engine import Engine

from app.schemas import ParsedLog

metadata = MetaData()

parsed_logs = Table(
    "parsed_logs",
    metadata,
    __import__("sqlalchemy").Column("id", Integer, primary_key=True),
    __import__("sqlalchemy").Column("template_id", String(64), index=True),
    __import__("sqlalchemy").Column("event_id", String(64), index=True),
    __import__("sqlalchemy").Column("timestamp", DateTime(timezone=True), index=True),
    __import__("sqlalchemy").Column("host", String(255), index=True),
    __import__("sqlalchemy").Column("block_id", String(255), nullable=True, index=True),
    __import__("sqlalchemy").Column("sequence_id", String(255), index=True),
    __import__("sqlalchemy").Column("template", String(2048)),
    __import__("sqlalchemy").Column("raw", String(4096)),
)

model_registry = Table(
    "model_registry",
    metadata,
    __import__("sqlalchemy").Column("id", Integer, primary_key=True),
    __import__("sqlalchemy").Column("name", String(255)),
    __import__("sqlalchemy").Column("version", String(64), index=True),
    __import__("sqlalchemy").Column("stage", String(64)),
    __import__("sqlalchemy").Column("artifact_uri", String(2048)),
    __import__("sqlalchemy").Column("metrics", JSON),
    __import__("sqlalchemy").Column("created_at", DateTime(timezone=True)),
)

feature_values = Table(
    "feature_values",
    metadata,
    __import__("sqlalchemy").Column("id", Integer, primary_key=True),
    __import__("sqlalchemy").Column("entity_id", String(255), index=True),
    __import__("sqlalchemy").Column("feature_name", String(255), index=True),
    __import__("sqlalchemy").Column("version", String(64), index=True),
    __import__("sqlalchemy").Column("timestamp", DateTime(timezone=True), index=True),
    __import__("sqlalchemy").Column("value", JSON),
)

inference_history = Table(
    "inference_history",
    metadata,
    __import__("sqlalchemy").Column("id", Integer, primary_key=True),
    __import__("sqlalchemy").Column("timestamp", DateTime(timezone=True)),
    __import__("sqlalchemy").Column("sequence_id", String(255), nullable=True),
    __import__("sqlalchemy").Column("actual_event", String(64), nullable=True),
    __import__("sqlalchemy").Column("anomaly", Integer),
    __import__("sqlalchemy").Column("confidence", Float),
    __import__("sqlalchemy").Column("latency_ms", Float),
    __import__("sqlalchemy").Column("payload", JSON),
)


class MetadataRepository:
    def __init__(self, dsn: str) -> None:
        self.engine: Engine = create_engine(dsn, pool_pre_ping=True)
        self.init_schema()

    def init_schema(self) -> None:
        metadata.create_all(self.engine)

    def save_parsed_log(self, parsed: ParsedLog) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                insert(parsed_logs).values(
                    template_id=parsed.template_id,
                    event_id=parsed.event_id,
                    timestamp=parsed.timestamp,
                    host=parsed.host,
                    block_id=parsed.block_id,
                    sequence_id=parsed.sequence_id,
                    template=parsed.template,
                    raw=parsed.raw,
                )
            )

    def save_inference(self, payload: dict[str, Any]) -> None:
        with self.engine.begin() as conn:
            conn.execute(insert(inference_history).values(**payload))

    def recent_anomalies(self, limit: int = 100) -> list[dict[str, Any]]:
        query = select(inference_history).where(inference_history.c.anomaly == 1).limit(limit)
        with self.engine.begin() as conn:
            return [dict(row._mapping) for row in conn.execute(query)]

    def register_model(
        self, name: str, version: str, stage: str, artifact_uri: str, metrics: dict[str, Any]
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                insert(model_registry).values(
                    name=name,
                    version=version,
                    stage=stage,
                    artifact_uri=artifact_uri,
                    metrics=metrics,
                    created_at=datetime.utcnow(),
                )
            )

    def get_recent_logs(self, limit: int = 1000) -> list[dict[str, Any]]:
        query = select(
            parsed_logs.c.template_id,
            parsed_logs.c.event_id,
            parsed_logs.c.timestamp,
            parsed_logs.c.host,
            parsed_logs.c.sequence_id
        ).order_by(parsed_logs.c.timestamp.desc()).limit(limit)
        with self.engine.begin() as conn:
            return [dict(row._mapping) for row in conn.execute(query)]

