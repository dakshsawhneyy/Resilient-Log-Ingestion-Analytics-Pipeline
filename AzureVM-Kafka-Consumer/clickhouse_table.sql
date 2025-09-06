CREATE TABLE log_stream (
    event_id UUID,
    timestamp DateTime,
    record String,
    metadata Nullable(String)
)
ENGINE = MergeTree 
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, event_id)

-- New Table
CREATE TABLE cloudfront_logs (
    event_id UUID,
    timestamp DateTime,
    ip String,
    status UInt16,
    method String,
    url_path String,
    edge_location String,
    user_agent String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, event_id);
