from __future__ import annotations

import dataclasses
import os 
from dotenv import load_dotenv

load_dotenv()

@dataclasses.dataclass(frozen=True)
class Settings:

    GCP_PROJECT: str = os.getenv("GCP_PROJECT", "")
    BQ_DATASET: str = os.getenv("BQ_DATASET", "")
    GCS_BUCKET: str = os.getenv("GCS_BUCKET", "")


def get_settings() -> Settings:
    s = Settings()
    if not s.GCP_PROJECT or not s.BQ_DATASET or not s.GCS_BUCKET:
        raise ValueError("Missing required environment variables for settings.")
    return s