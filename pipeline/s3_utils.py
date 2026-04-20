"""
S3 utility helpers for the credit-card lake pipeline.

Provides drop-in replacements for local filesystem operations:
  s3_exists()            ← os.path.exists()
  s3_write_parquet_new() ← write to S3, raise if already exists
  s3_overwrite_parquet() ← write to S3, overwrite silently
  s3_read_parquet()      ← pd.read_parquet() from S3
  s3_glob()              ← glob.glob() over S3 prefix
  s3_delete()            ← os.remove()
  s3_copy()              ← used for Gold atomic rename (copy + delete)
  get_duckdb_s3_conn()   ← duckdb.connect() with httpfs + S3 credentials

All paths use the form 's3://<bucket>/<key>'.
BUCKET is resolved once at import time from the S3_BUCKET env var
(default: credit-card-lake).
"""
import os

import boto3
import duckdb
import s3fs
from botocore.exceptions import ClientError

BUCKET: str = os.environ.get('S3_BUCKET', 'credit-card-lake')


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_s3(s3_path: str) -> tuple[str, str]:
    """Split 's3://bucket/key' → (bucket, key)."""
    assert s3_path.startswith('s3://'), f"Not an S3 path: {s3_path}"
    rest = s3_path[5:]
    bucket, _, key = rest.partition('/')
    return bucket, key


def _s3():
    return boto3.client('s3')


def _fs():
    return s3fs.S3FileSystem()


# ---------------------------------------------------------------------------
# Existence check
# ---------------------------------------------------------------------------

def s3_exists(s3_path: str) -> bool:
    """Return True if the S3 object exists, False if 404/NoSuchKey."""
    bucket, key = _parse_s3(s3_path)
    try:
        _s3().head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = exc.response['Error']['Code']
        if code in ('404', 'NoSuchKey'):
            return False
        raise


# ---------------------------------------------------------------------------
# Parquet read / write
# ---------------------------------------------------------------------------

def s3_read_parquet(s3_path: str):
    """Read a Parquet file from S3 into a pandas DataFrame."""
    import pandas as pd
    return pd.read_parquet(s3_path)


def s3_write_parquet_new(df, s3_path: str) -> int:
    """
    Write *df* to *s3_path* as Parquet.
    Raises RuntimeError if the object already exists (idempotency guard).
    Returns the number of rows written.
    """
    if s3_exists(s3_path):
        raise RuntimeError(f"Target path already exists: {s3_path}")
    df.to_parquet(s3_path, index=False)
    return len(df)


def s3_overwrite_parquet(df, s3_path: str) -> int:
    """
    Write *df* to *s3_path* as Parquet, overwriting any existing object.
    Used for control.parquet and run_log.parquet where atomic replacement
    is the correct behaviour (S3 PUT is effectively atomic).
    Returns the number of rows written.
    """
    df.to_parquet(s3_path, index=False)
    return len(df)


# ---------------------------------------------------------------------------
# Glob
# ---------------------------------------------------------------------------

def s3_glob(pattern: str) -> list[str]:
    """
    Return a list of 's3://...' paths matching *pattern*.
    Supports single-level '*' and recursive '**' wildcards.

    Example:
        s3_glob('s3://credit-card-lake/data/silver/transactions/**/*.parquet')
    """
    fs = _fs()
    path = pattern[5:]          # strip 's3://'
    matches = fs.glob(path)
    return [f's3://{m}' for m in matches]


# ---------------------------------------------------------------------------
# Delete / copy (for Gold atomic rename)
# ---------------------------------------------------------------------------

def s3_delete(s3_path: str) -> None:
    """Delete a single S3 object."""
    bucket, key = _parse_s3(s3_path)
    _s3().delete_object(Bucket=bucket, Key=key)


def s3_copy(src: str, dst: str) -> None:
    """
    Copy an S3 object from *src* to *dst* within the same or different bucket.
    Used by pipeline.py to simulate atomic Gold rename:
        s3_copy(staging_path, canonical_path)
        s3_delete(staging_path)
    """
    src_bucket, src_key = _parse_s3(src)
    dst_bucket, dst_key = _parse_s3(dst)
    _s3().copy_object(
        CopySource={'Bucket': src_bucket, 'Key': src_key},
        Bucket=dst_bucket,
        Key=dst_key,
    )


# ---------------------------------------------------------------------------
# DuckDB connection with httpfs
# ---------------------------------------------------------------------------

def get_duckdb_s3_conn() -> duckdb.DuckDBPyConnection:
    """
    Return an in-memory DuckDB connection with the httpfs extension loaded
    and S3 credentials injected from environment variables.

    Use this everywhere a DuckDB query must read from or write to S3.
    """
    conn = duckdb.connect()
    conn.execute("INSTALL httpfs; LOAD httpfs;")

    region = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
    conn.execute(f"SET s3_region='{region}';")

    key    = os.environ.get('AWS_ACCESS_KEY_ID', '')
    secret = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
    token  = os.environ.get('AWS_SESSION_TOKEN', '')

    if key:
        conn.execute(f"SET s3_access_key_id='{key}';")
    if secret:
        conn.execute(f"SET s3_secret_access_key='{secret}';")
    if token:
        conn.execute(f"SET s3_session_token='{token}';")

    return conn
