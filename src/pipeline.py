import io
import json
import pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery
from config.settings import Settings
from gcs.gcs_integrations import list_blobs, download_blob_as_string
from gcs.paths import app_events_prefix, provider_exports_prefix, refunds_prefix
from utils.commons import read_jsonl
from transforms.normalize_events import normalize_app_event
from transforms.normalize_providers import normalize_refund, normalize_provider_payment
from transforms.orders import normalize_order
from transforms.dedupe import dedupe_by_key
from config.settings import Settings
from bq.bq_integrations import load_df_to_table
def _now_ts():
    return datetime.now(timezone.utc)

def ingest_app_events(settings: Settings, bq: bigquery.Client, date: str, hour: str) -> int:
    prefix = app_events_prefix(date, hour)
    blobs = list_blobs(settings.GCS_BUCKET, prefix)
    rows = []
    for b in blobs:
        text = download_blob_as_string(settings.GCS_BUCKET, b)
        for r in read_jsonl(text):
            rows.append(normalize_app_event(r, default_tz=settings.DEFAULT_TZ))
    rows = dedupe_by_key(rows, "event_id")
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    df['_ingested_at'] = _now_ts()
    df['raw'] = df['raw'].apply(lambda x: json.dumps(x, ensure_ascii=False))

    table_stg = f"{settings.GCP_PROJECT}.{settings.BQ_STAGING_DATASET}.app_events_staging"
    load_df_to_table(bq, df, table_stg)

    # Deduplication and merge to gold table
    table_gold = f"{settings.GCP_PROJECT}.{settings.BQ_GOLD_DATASET}.app_events"
    load_df_to_table(bq, df, table_gold, write_disposition="WRITE_APPEND")
    return len(rows)

def ingest_orders(settings: Settings, bq: bigquery.Client, date: str, hour: str) -> int:
    prefix = provider_exports_prefix(date, hour)
    blobs = list_blobs(settings.GCS_BUCKET, prefix)
    rows = []
    for b in blobs:
        text = download_blob_as_string(settings.GCS_BUCKET, b)
        for r in read_jsonl(text):
            rows.append(normalize_order(r, default_tz=settings.DEFAULT_TZ))
    rows = dedupe_by_key(rows, "order_id")
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    df['_ingested_at'] = _now_ts()
    df['raw'] = df['raw'].apply(lambda x: json.dumps(x, ensure_ascii=False))

    table_stg = f"{settings.GCP_PROJECT}.{settings.BQ_STAGING_DATASET}.orders_staging"
    load_df_to_table(bq, df, table_stg)

    # Deduplication and merge to gold table
    table_gold = f"{settings.GCP_PROJECT}.{settings.BQ_GOLD_DATASET}.orders"
    load_df_to_table(bq, df, table_gold, write_disposition="WRITE_APPEND")
    return len(rows)

def ingest_provider_exports(settings: Settings, bq: bigquery.Client, provider: str, date: str) -> int:
    prefix = provider_exports_prefix(provider, date)
    blobs = list_blobs(settings.GCS_BUCKET, prefix)
    payment_rows = []
    refund_rows = []
    for b in blobs:
        if b.endswith('.csv'):
            df_in = pd.read_csv(io.StringIO(download_blob_as_string(settings.GCS_BUCKET, b)))
            for _, rec in df_in.iterrows():
                payment_rows.append(normalize_provider_payment(rec, provider, default_tz=settings.DEFAULT_TZ))
        elif b.endswith('.jsonl'):
            text = download_blob_as_string(settings.GCS_BUCKET, b)
            for r in read_jsonl(text):
                refund_rows.append(normalize_refund(r, provider, default_tz=settings.DEFAULT_TZ))
    payment_rows = dedupe_by_key(payment_rows, "payment_id")
    if not payment_rows:
        return 0
    df_payments = pd.DataFrame(payment_rows)
    df_payments['_ingested_at'] = _now_ts()
    df_payments['raw'] = df_payments['raw'].apply(lambda x: json.dumps(x, ensure_ascii=False))


    table_stg_payments = f"{settings.GCP_PROJECT}.{settings.BQ_STAGING_DATASET}.provider_exports_staging"
    load_df_to_table(bq, df_payments, table_stg_payments)

    table_gold_payments = f"{settings.GCP_PROJECT}.{settings.BQ_GOLD_DATASET}.provider_payments"
    load_df_to_table(bq, df_payments, table_gold_payments)


def ingest_refunds(settings: Settings, bq: bigquery.Client, provider: str, date: str) -> int:
    prefix = refunds_prefix(provider, date)
    blobs = list_blobs(settings.GCS_BUCKET, prefix)
    refund_rows = []
    for b in blobs:
        text = download_blob_as_string(settings.GCS_BUCKET, b)
        if b.endswith('.csv'):
            df_in = pd.read_csv(io.StringIO(text))
            for _, rec in df_in.iterrows():
                refund_rows.append(normalize_refund(rec, provider, default_tz=settings.DEFAULT_TZ))
        else:
            for r in read_jsonl(text):
                refund_rows.append(normalize_refund(r, provider, default_tz=settings.DEFAULT_TZ))
    refund_rows = dedupe_by_key(refund_rows, "refund_id")
    if not refund_rows:
        return 0
    df_refunds = pd.DataFrame(refund_rows)
    df_refunds['_ingested_at'] = _now_ts()
    df_refunds['raw'] = df_refunds['raw'].apply(lambda x: json.dumps(x, ensure_ascii=False))

    table_stg_refunds = f"{settings.GCP_PROJECT}.{settings.BQ_STAGING_DATASET}.refunds_staging"
    load_df_to_table(bq, df_refunds, table_stg_refunds)

    table_gold_refunds = f"{settings.GCP_PROJECT}.{settings.BQ_GOLD_DATASET}.refunds"
    load_df_to_table(bq, df_refunds, table_gold_refunds)