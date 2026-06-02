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
        from pyflink.datastream.functions import MapFunction
    except ImportError as exc:
        raise RuntimeError("Install the streaming extra to run the PyFlink job") from exc

    class LogParserMapFunction(MapFunction):
        def __init__(self) -> None:
            self.settings = None
            self.parser = None
            self.windows = None

        def open(self, runtime_context) -> None:
            from app.config import get_settings
            from app.parser import HDFSLogParser
            from app.processing import SlidingWindowBuilder
            self.settings = get_settings()
            self.parser = HDFSLogParser()
            self.windows = SlidingWindowBuilder(self.settings.sequence_length)

        def map(self, value: str) -> str:
            from app.schemas import RawLog
            from app.features import extract_window_features
            parsed = self.parser.parse(RawLog(message=value))
            window = self.windows.add(parsed)
            result = {
                "message_type": "feature_window" if window is not None else "parsed_log",
                "parsed": parsed.model_dump(mode="json"),
                "window": None if window is None else window.__dict__,
                "features": None if window is None else extract_window_features(window),
            }
            return json.dumps(result, default=str)

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
        LogParserMapFunction()
    ).sink_to(sink)
    env.execute("hdfs-log-drain3-feature-inference")



if __name__ == "__main__":
    main()
