CREATE TABLE IF NOT EXISTS `{project}.{staging_dataset}.app_events_staging`
(
  event_id STRING,
  event_name STRING,
  user_id STRING,
  session_id STRING,
  order_id STRING,
  payment_intent_id STRING,
  event_ts_utc TIMESTAMP,
  country STRING,
  device STRING,
  utm_source STRING,
  utm_medium STRING,
  utm_campaign STRING,
  raw JSON,
  _ingested_at TIMESTAMP
)
PARTITION BY DATE(_ingested_at);

CREATE TABLE IF NOT EXISTS `{project}.{staging_dataset}.provider_exports_staging`
(
  export_id STRING,
  provider_name STRING,
  export_ts_utc TIMESTAMP,
  raw JSON,
  _ingested_at TIMESTAMP
)
PARTITION BY DATE(_ingested_at);

CREATE TABLE IF NOT EXISTS `{project}.{staging_dataset}.refunds_staging`
(
  refund_id STRING,
  order_id STRING,
  payment_intent_id STRING,
  refund_ts_utc TIMESTAMP,
  amount FLOAT64,
  currency STRING,
  raw JSON,
  _ingested_at TIMESTAMP
)
PARTITION BY DATE(_ingested_at);

CREATE TABLE IF NOT EXISTS `{project}.{staging_dataset}.orders_staging`
(
  order_id STRING,
  user_id STRING,
  email STRING,
  phone STRING,
  country STRING,
  product_id STRING,
  product_name STRING,
  order_amount FLOAT64,
  currency STRING,
  order_ts_utc TIMESTAMP,
  status STRING,
  raw JSON,
  _ingested_at TIMESTAMP
)
PARTITION BY DATE(_ingested_at);
