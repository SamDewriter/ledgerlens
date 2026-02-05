from __future__ import annotations
from google.cloud import bigquery
import pandas as pd


def get_bq_client(project) -> bigquery.Client:
    return bigquery.Client(project=project)

def load_df_to_table(
        client: bigquery.Client,
        df: pd.DataFrame,
        table_id: str,
        write_disposition: str = "WRITE_APPEND  ",
) -> None:
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
    )
    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config,
    )
    job.result()



def ensure_dataset(client: bigquery.Client, dataset_id: str, location: str = "US") -> None:
    ds = bigquery.Dataset(dataset_id)
    ds.location = location
    try:
        client.create_dataset(ds)
    except Exception:
        client.create_dataset(ds, exists_ok=True)


def run_sql(client: bigquery.Client, sql: str) -> bigquery.table.RowIterator:
    query_job = client.query(sql)
    return query_job.result()