from datetime import UTC, datetime
from typing import Any

from sqlalchemy import create_engine, insert, select

from app.db.metadata import feature_values, metadata


class FeatureStore:
    def __init__(self, postgres_dsn: str) -> None:
        self.engine = create_engine(postgres_dsn, pool_pre_ping=True)

    def init_schema(self) -> None:
        metadata.create_all(self.engine)

    def write_feature(
        self,
        entity_id: str,
        feature_name: str,
        value: Any,
        version: str = "v1",
        timestamp: datetime | None = None,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                insert(feature_values).values(
                    entity_id=entity_id,
                    feature_name=feature_name,
                    value=value,
                    version=version,
                    timestamp=timestamp or datetime.now(UTC),
                )
            )

    def read_feature(self, entity_id: str, feature_name: str, version: str | None = None) -> Any:
        query = (
            select(feature_values.c.value)
            .where(feature_values.c.entity_id == entity_id)
            .where(feature_values.c.feature_name == feature_name)
            .order_by(feature_values.c.timestamp.desc())
            .limit(1)
        )
        if version:
            query = query.where(feature_values.c.version == version)
        with self.engine.begin() as conn:
            row = conn.execute(query).first()
            return None if row is None else row[0]

    def versioning(self) -> list[str]:
        query = select(feature_values.c.version).distinct().order_by(feature_values.c.version)
        with self.engine.begin() as conn:
            return [str(row[0]) for row in conn.execute(query)]
