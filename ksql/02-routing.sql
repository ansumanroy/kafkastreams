-- One persistent query per target topic.
-- These queries mirror the static routes in k8s/app/router-config.json.

CREATE STREAM route_acdw
  WITH (KAFKA_TOPIC='ACDW', VALUE_FORMAT='KAFKA') AS
  SELECT payload
  FROM ingest_enriched
  WHERE UCASE(target) = 'ACDW'
  EMIT CHANGES;

CREATE STREAM route_mulesoft
  WITH (KAFKA_TOPIC='MULESOFT', VALUE_FORMAT='KAFKA') AS
  SELECT payload
  FROM ingest_enriched
  WHERE UCASE(target) = 'MULESOFT'
  EMIT CHANGES;

CREATE STREAM route_gdw
  WITH (KAFKA_TOPIC='GDW', VALUE_FORMAT='KAFKA') AS
  SELECT payload
  FROM ingest_enriched
  WHERE UCASE(target) = 'GDW'
  EMIT CHANGES;

CREATE STREAM route_siebel
  WITH (KAFKA_TOPIC='SIEBEL', VALUE_FORMAT='KAFKA') AS
  SELECT payload
  FROM ingest_enriched
  WHERE UCASE(target) = 'SIEBEL'
  EMIT CHANGES;
