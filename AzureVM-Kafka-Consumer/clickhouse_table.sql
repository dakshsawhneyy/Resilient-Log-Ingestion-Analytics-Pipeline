CREATE TABLE log_streams (
    event_id UUID,
    timestamp DateTime MATERIALIZED now(),
    record String,
    metadata Nullable(String)
)
ENGINE = MergeTree PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, event_id)