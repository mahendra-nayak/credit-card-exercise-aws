"""
pipeline/run_log.py — Run log writer (S3 storage variant).

Provides:
  write_run_log_row() — atomic append to s3://credit-card-lake/data/pipeline/run_log.parquet
  sanitise_error()    — strip filesystem paths, enforce max length

S3 atomicity pattern: write to temp S3 key, then s3_copy → canonical, s3_delete temp.
"""
import re
from datetime import datetime, timezone

import pandas as pd

from pipeline.s3_utils import s3_copy, s3_delete, s3_exists, s3_read_parquet

RUN_LOG_PATH  = 's3://credit-card-lake/data/pipeline/run_log.parquet'
TEMP_LOG_PATH = 's3://credit-card-lake/data/pipeline/.tmp_run_log.parquet'

VALID_PIPELINE_TYPES = frozenset({'HISTORICAL', 'INCREMENTAL'})
VALID_LAYERS         = frozenset({'BRONZE', 'SILVER', 'GOLD'})
VALID_STATUSES       = frozenset({'SUCCESS', 'FAILED', 'SKIPPED'})


def sanitise_error(raw: str, max_chars: int = 500) -> str:
    """
    Strip filesystem path substrings (anything starting with '/') and
    enforce a maximum character length.

    Returns 'unknown error' if the cleaned string is empty.
    """
    cleaned = re.sub(r'/\S*', '', raw or '').strip()
    if len(cleaned) > max_chars:
        suffix = '[truncated]'
        cleaned = cleaned[:max_chars - len(suffix)] + suffix
    return cleaned or 'unknown error'


def write_run_log_row(
    run_id: str,
    model_name: str,
    layer: str,
    *,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
    pipeline_type: str,
    status: str,
    records_processed: int | None = None,
    records_written: int | None = None,
    records_rejected: int | None = None,
    error_message: str | None = None,
) -> None:
    """
    Atomically append one row to the S3 run log.

    Validation rules (raise ValueError on violation):
      - pipeline_type must be in VALID_PIPELINE_TYPES
      - layer must be in VALID_LAYERS
      - status must be in VALID_STATUSES
      - FAILED requires error_message; non-FAILED forbids it
      - records_rejected is only allowed for SILVER layer
    """
    # --- validation ---
    if pipeline_type not in VALID_PIPELINE_TYPES:
        raise ValueError(f'Invalid pipeline_type: {pipeline_type!r}. Must be one of {VALID_PIPELINE_TYPES}')
    if layer not in VALID_LAYERS:
        raise ValueError(f'Invalid layer: {layer!r}. Must be one of {VALID_LAYERS}')
    if status not in VALID_STATUSES:
        raise ValueError(f'Invalid status: {status!r}. Must be one of {VALID_STATUSES}')
    if status == 'FAILED' and error_message is None:
        raise ValueError('error_message is required when status=FAILED')
    if status != 'FAILED' and error_message is not None:
        raise ValueError(f'error_message must be None when status={status!r}')
    if layer != 'SILVER' and records_rejected is not None:
        raise ValueError('records_rejected is only allowed for SILVER layer')

    # --- build row ---
    new_row = pd.DataFrame([{
        'run_id':             run_id,
        'pipeline_type':      pipeline_type,
        'model_name':         model_name,
        'layer':              layer,
        'started_at':         started_at,
        'completed_at':       completed_at,
        'status':             status,
        'records_processed':  records_processed,
        'records_written':    records_written,
        'records_rejected':   records_rejected,
        'error_message':      error_message,
    }])

    # --- atomic S3 append ---
    if s3_exists(RUN_LOG_PATH):
        existing = s3_read_parquet(RUN_LOG_PATH)
        combined = pd.concat([existing, new_row], ignore_index=True)
    else:
        combined = new_row

    # Write to temp, then copy → canonical, delete temp (INV-37, INV-37b)
    combined.to_parquet(TEMP_LOG_PATH, index=False)
    s3_copy(TEMP_LOG_PATH, RUN_LOG_PATH)
    s3_delete(TEMP_LOG_PATH)
