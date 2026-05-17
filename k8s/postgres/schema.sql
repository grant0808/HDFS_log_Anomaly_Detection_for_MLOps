CREATE TABLE IF NOT EXISTS parsed_logs (
  id SERIAL PRIMARY KEY,
  template_id TEXT,
  event_id TEXT,
  timestamp TIMESTAMPTZ,
  host TEXT,
  block_id TEXT,
  sequence_id TEXT,
  template TEXT,
  raw TEXT
);
