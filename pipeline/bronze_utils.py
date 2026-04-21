"""
pipeline/bronze_utils.py — Bronze layer shared utilities (S3 storage variant).

Four functions used by all Bronze loaders:
  read_csv_source      — read a CSV from S3, add _source_row_number
  add_audit_columns    — attach _source_file, _ingested_at, _pipeline_run_id
  write_parquet_atomic — write Parquet to S3 via temp-key-then-copy pattern
  assert_row_count     — verify S3 Parquet row count via DuckDB httpfs

S3 atomic write (INV-05a/b/c):
  Writes to .tmp_{filename} key in the same S3 prefix, then copies to the
  canonical key and deletes the temp key.  A dot-prefixed temp key is invisible
  to DuckDB wildcard scans (INV-05c) and lives in the same prefix as the target
  (INV-05b).  The canonical key is never visible until the copy completes
  (INV-05a).
"""
import os
from datetime import datetime

import pandas

from pipeline.s3_utils import (
    get_duckdb_s3_conn,
    s3_copy,
    s3_delete,
    s3_exists,
    s3_write_parquet_new,
)

TEMP_PREFIX = '.tmp_'          # INV-05c: dot-prefixed hidden temp key


def read_csv_source(s3_path: str) -> pandas.DataFrame:
    """
    Read all rows from a CSV stored at *s3_path* (s3://…).
    No filtering, no dropna, no drop_duplicates — every source row is returned.
    Adds _source_row_number as a 1-based sequential integer.
    Raises FileNotFoundError if the S3 object does not exist.
    """
    if not s3_exists(s3_path):
        raise FileNotFoundError(f"Source file not found: {s3_path}")
    df = pandas.read_csv(s3_path, dtype=str, keep_default_na=False)
    df = df.reset_index()
    df = df.rename(columns={'index': '_source_row_number'})
    df['_source_row_number'] = df['_source_row_number'] + 1
    return df


def add_audit_columns(
    df: pandas.DataFrame,
    source_file: str,
    run_id: str,
    ingested_at: datetime,
) -> pandas.DataFrame:
    """
    Attach the three Bronze audit columns to *df*.
    Raises ValueError if any of the columns already exists (INV-44).

    _source_file     — os.path.basename(source_file), filename only (INV-44b)
    _ingested_at     — passed in, not generated here
    _pipeline_run_id — passed in, not generated here
    """
    for col in ('_source_file', '_ingested_at', '_pipeline_run_id'):
        if col in df.columns:
            raise ValueError(f"Column already exists in DataFrame: {col}")
    df['_source_file'] = os.path.basename(source_file)
    df['_ingested_at'] = ingested_at
    df['_pipeline_run_id'] = run_id
    return df


def write_parquet_atomic(
    df: pandas.DataFrame,
    s3_dir: str,
    filename: str = 'data.parquet',
) -> int:
    """
    Write *df* as Parquet to *s3_dir*/*filename* atomically.

    S3 atomic write sequence (mirrors local temp-rename pattern):
      1. Raise RuntimeError if the canonical S3 key already exists (INV-06).
      2. Write df to .tmp_{filename} in the same S3 prefix (INV-05b, INV-05c).
      3. Copy temp key → canonical key.
      4. Delete temp key.

    The canonical key is never visible until step 3 completes (INV-05a).
    Returns the number of rows written.
    """
    canonical = f"{s3_dir}/{filename}"
    temp      = f"{s3_dir}/{TEMP_PREFIX}{filename}"

    if s3_exists(canonical):
        raise RuntimeError(f"Target path already exists: {canonical}")

    df.to_parquet(temp, index=False)      # write to temp key
    s3_copy(temp, canonical)              # promote to canonical
    s3_delete(temp)                       # remove temp key
    return len(df)


def assert_row_count(s3_path: str, expected: int) -> None:
    """
    Query *s3_path* via DuckDB httpfs and assert the row count equals *expected*.
    Raises AssertionError with a descriptive message on mismatch (INV-03).
    """
    conn  = get_duckdb_s3_conn()
    count = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{s3_path}')"
    ).fetchone()[0]
    conn.close()
    if count != expected:
        raise AssertionError(
            f"row count mismatch: parquet={count} expected={expected}"
        )
