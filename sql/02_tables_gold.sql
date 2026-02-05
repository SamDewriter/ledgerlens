CREATE TABLE IF NOT EXISTS `{project}.{dataset}.app_events`
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
PARTITION BY DATE(_ingested_at)
CLUSTER BY event_name, country, user_id;

CREATE TABLE IF NOT EXISTS `{project}.{dataset}.provider_payments`
(
  provider STRING,
  payment_id STRING,
  order_id STRING,
  status STRING,
  amount FLOAT64,
  currency STRING,
  payment_ts_utc TIMESTAMP,
  email STRING,
  phone STRING,
  raw JSON,
  _ingested_at TIMESTAMP
)
PARTITION BY DATE(_ingested_at)
CLUSTER BY provider, status;


CREATE TABLE IF NOT EXISTS `{project}.{dataset}.orders`
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
PARTITION BY DATE(_ingested_at)
CLUSTER BY country, product_id;

CREATE TABLE IF NOT EXISTS `{project}.{dataset}.refunds`
(
  provider STRING,
  refund_id STRING,
  payment_id STRING,
  amount FLOAT64,
  currency STRING,
  refund_ts_utc TIMESTAMP,
  reason STRING,
  raw JSON,
  _ingested_at TIMESTAMP
)
PARTITION BY DATE(_ingested_at)
CLUSTER BY provider;

