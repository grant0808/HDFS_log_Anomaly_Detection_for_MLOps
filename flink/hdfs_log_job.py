import json
import os

from app.config import get_settings
from app.features import extract_window_features
from app.parser import HDFSLogParser
from app.processing import SlidingWindowBuilder
from app.schemas import RawLog


def process_log_line(message: str) -> dict[str, object] | None:
    settings = get_settings()
    parser = HDFSLogParser()
    windows = SlidingWindowBuilder(settings.sequence_length)
    parsed = parser.parse(RawLog(message=message))
    window = windows.add(parsed)
    return {
        "message_type": "feature_window" if window is not None else "parsed_log",
        "parsed": parsed.model_dump(mode="json"),
        "window": None if window is None else window.__dict__,
        "features": None if window is None else extract_window_features(window),
    }


def main() -> None:
    try:
        from pyflink.common import SimpleStringSchema, WatermarkStrategy
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaRecordSerializationSchema
        from pyflink.datastream.connectors.kafka import KafkaSink, KafkaSource
    except ImportError as exc:
        raise RuntimeError("Install the streaming extra to run the PyFlink job") from exc

    settings = get_settings()
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.getenv("FLINK_PARALLELISM", "1")))
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(settings.kafka_bootstrap_servers)
        .set_topics(settings.raw_topic)
        .set_group_id("hdfs-drain3-parser")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(settings.kafka_bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(settings.feature_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )
    env.from_source(source, WatermarkStrategy.no_watermarks(), "hdfs-raw-logs").map(
        lambda value: json.dumps(process_log_line(value), default=str)
    ).sink_to(sink)
    env.execute("hdfs-log-drain3-feature-inference")


if __name__ == "__main__":
    main()
