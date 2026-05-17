import time

from confluent_kafka import Producer

from app.config import get_settings


def main() -> None:
    settings = get_settings()
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
    for line in open("data/sample/hdfs.log", encoding="utf-8"):
        producer.produce(settings.raw_topic, line.strip().encode("utf-8"))
        producer.poll(0)
        time.sleep(0.05)
    producer.flush()


if __name__ == "__main__":
    main()
