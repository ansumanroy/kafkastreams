-- Source stream over raw string payloads from Ingest.
-- VALUE_FORMAT=KAFKA preserves the original string and allows routing malformed JSON to DLQ.
CREATE STREAM ingest_raw (
  payload VARCHAR
) WITH (
  KAFKA_TOPIC='Ingest',
  VALUE_FORMAT='KAFKA'
);

-- Derive target from JSON field; malformed/non-JSON payloads produce NULL target.
CREATE STREAM ingest_enriched AS
  SELECT
    payload,
    EXTRACTJSONFIELD(payload, '$.target') AS target
  FROM ingest_raw
  EMIT CHANGES;
