from prometheus_client import Counter, Gauge, Histogram

LOGS_PROCESSED = Counter("logs_processed_total", "Total processed HDFS logs")
ANOMALIES = Counter("anomalies_total", "Total anomaly predictions")
MODEL_FALLBACKS = Counter("model_fallbacks_total", "Total rule fallback predictions")
ANOMALY_RATE = Gauge("anomaly_rate", "Recent anomaly rate")
INFERENCE_LATENCY = Histogram(
    "inference_latency", "Inference latency in seconds", buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2)
)
DATA_DRIFT_SCORE = Gauge("data_drift_score", "Latest data drift score")
CONSUMER_LAG = Gauge("consumer_lag", "Kafka consumer lag")
UNKNOWN_TEMPLATE_COUNT = Counter("unknown_template_count", "Unknown parser templates")
MODEL_VERSION = Gauge("model_version", "Numeric model version label", ["version"])
PREDICTION_CONFIDENCE = Gauge("prediction_confidence", "Latest prediction confidence")
