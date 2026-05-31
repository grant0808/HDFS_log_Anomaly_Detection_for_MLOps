from typing import Any

from app.config import Settings


class ClickHouseHistoryWriter:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = None

    def connect(self) -> None:
        import clickhouse_connect

        self.client = clickhouse_connect.get_client(
            host=self.settings.clickhouse_host,
            port=self.settings.clickhouse_port,
            username=self.settings.clickhouse_user,
            password=self.settings.clickhouse_password,
            database=self.settings.clickhouse_database,
        )

    def init_schema(self) -> None:
        if self.client is None:
            self.connect()
        assert self.client is not None
        self.client.command(
            """
            CREATE TABLE IF NOT EXISTS inference_history (
              timestamp DateTime,
              sequence_id String,
              actual_event String,
              anomaly UInt8,
              confidence Float32,
              latency_ms Float32,
              model_version String
            ) ENGINE = MergeTree ORDER BY (timestamp, sequence_id)
            """
        )

    def write_inference(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        if self.client is None:
            self.connect()
        assert self.client is not None
        self.client.insert(
            "inference_history",
            [
                [
                    row["timestamp"],
                    row.get("sequence_id") or "",
                    row.get("actual_event") or "",
                    int(row.get("anomaly", False)),
                    float(row.get("confidence", 0.0)),
                    float(row.get("latency_ms", 0.0)),
                    row.get("model_version", ""),
                ]
                for row in rows
            ],
            column_names=[
                "timestamp",
                "sequence_id",
                "actual_event",
                "anomaly",
                "confidence",
                "latency_ms",
                "model_version",
            ],
        )
