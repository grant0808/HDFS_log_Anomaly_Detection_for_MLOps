CREATE TABLE IF NOT EXISTS inference_history (
  timestamp DateTime,
  sequence_id String,
  actual_event String,
  anomaly UInt8,
  confidence Float32,
  latency_ms Float32,
  model_version String
) ENGINE = MergeTree ORDER BY (timestamp, sequence_id);
