from __future__ import annotations
from typing import List
from google.cloud import storage


def get_storage_client(project: str) -> storage.Client:
    return storage.Client(project=project)

def list_blobs(bucket: str, prefix: str) -> List[str]:
    client = get_storage_client()
    return [b.name for b in client.list_blobs(bucket, prefix=prefix) if not b.name.endswith("/")]


def download_blob_as_string(bucket: str, blob_name: str) -> str:
    client = get_storage_client()
    bucket_obj = client.bucket(bucket)
    blob = bucket_obj.blob(blob_name)
    return blob.download_as_text()

def upload_string_as_blob(bucket: str, blob_name: str, data: str) -> None:
    client = get_storage_client()
    bucket_obj = client.bucket(bucket)
    blob = bucket_obj.blob(blob_name)
    blob.upload_from_string(data)


def upload_file(bucket: str, blob_name: str, file_path: str) -> None:
    client = get_storage_client()
    bucket_obj = client.bucket(bucket)
    blob = bucket_obj.blob(blob_name)
    blob.upload_from_filename(file_path)


