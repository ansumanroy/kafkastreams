-- Dead-letter query for malformed, missing-target, blank-target, or unknown-target records.
CREATE STREAM route_dlq
  WITH (KAFKA_TOPIC='Ingest-dlq', VALUE_FORMAT='KAFKA') AS
  SELECT payload
  FROM ingest_enriched
  WHERE target IS NULL
     OR TRIM(target) = ''
     OR UCASE(target) NOT IN ('ACDW', 'MULESOFT', 'GDW', 'SIEBEL')
  EMIT CHANGES;
