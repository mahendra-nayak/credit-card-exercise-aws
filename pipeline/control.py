"""
pipeline/control.py — Watermark control (S3 storage variant).

Provides:
  read_watermark()          → datetime.date | None
  write_watermark(date, run_id) → None  (always single-row overwrite — INV-33b)

control.parquet lives at s3://credit-card-lake/data/pipeline/control.parquet.
Writes go through a temp S3 key then s3_copy → canonical (atomic overwrite).
"""
import datetime
from datetime import timezone

import pandas as pd

from pipeline.s3_utils import s3_copy, s3_delete, s3_exists, s3_read_parquet

CONTROL_PATH      = 's3://credit-card-lake/data/pipeline/control.parquet'
TEMP_CONTROL_PATH = 's3://credit-card-lake/data/pipeline/.tmp_control.parquet'


def read_watermark() -> datetime.date | None:
    """
    Return the last successfully processed date, or None if no watermark exists.
    Raises ValueError if control.parquet exists but does not contain exactly 1 row.
    """
    if not s3_exists(CONTROL_PATH):
        return None
    df = s3_read_parquet(CONTROL_PATH)
    if len(df) != 1:
        raise ValueError(f'control.parquet must have exactly 1 row, has {len(df)}')
    return pd.Timestamp(df['last_processed_date'].iloc[0]).date()


def write_watermark(date: datetime.date, run_id: str) -> None:
    """
    Write a fresh single-row control.parquet with *date* as the watermark.

    Always replaces — never appends (INV-33b).
    Atomic overwrite via temp S3 key: write temp → s3_copy → s3_delete temp.
    """
    df = pd.DataFrame([{
        'last_processed_date': date,
        'updated_at':          datetime.datetime.now(timezone.utc),
        'updated_by_run_id':   run_id,
    }])
    df.to_parquet(TEMP_CONTROL_PATH, index=False)
    s3_copy(TEMP_CONTROL_PATH, CONTROL_PATH)
    s3_delete(TEMP_CONTROL_PATH)
