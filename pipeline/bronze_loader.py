"""
pipeline/bronze_loader.py — Bronze layer loaders (S3 storage variant).

Three public functions:
  load_bronze_transactions(target_date, run_id) — date-partitioned
  load_bronze_accounts(target_date, run_id)     — date-partitioned
  load_bronze_transaction_codes(run_id)         — non-date-partitioned

Source CSVs are read from s3://credit-card-lake/source/.
Bronze Parquet files are written to s3://credit-card-lake/data/bronze/.
"""
from datetime import datetime, timezone

from pipeline.bronze_utils import (
    add_audit_columns,
    assert_row_count,
    read_csv_source,
    write_parquet_atomic,
)
from pipeline.s3_utils import s3_exists

BUCKET_ROOT = 's3://credit-card-lake'
SOURCE_ROOT  = f'{BUCKET_ROOT}/source'
BRONZE_ROOT  = f'{BUCKET_ROOT}/data/bronze'


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------

def load_bronze_transactions(target_date: str, run_id: str) -> int:
    """
    Load transactions CSV for *target_date* into Bronze.

    Source : s3://credit-card-lake/source/transactions_{target_date}.csv
    Target : s3://credit-card-lake/data/bronze/transactions/date={target_date}/data.parquet

    Returns the number of rows written.
    Raises FileNotFoundError if the source CSV is absent.
    Raises RuntimeError if the target Parquet already exists (idempotency).
    """
    csv_path   = f'{SOURCE_ROOT}/transactions_{target_date}.csv'
    target_dir = f'{BRONZE_ROOT}/transactions/date={target_date}'

    ingested_at = datetime.now(timezone.utc)
    df = read_csv_source(csv_path)                              # raises FileNotFoundError if missing
    df = add_audit_columns(df, csv_path, run_id, ingested_at)
    row_count = write_parquet_atomic(df, target_dir)            # raises RuntimeError if exists
    assert_row_count(f'{target_dir}/data.parquet', row_count)
    return row_count


# ---------------------------------------------------------------------------
# Accounts
# ---------------------------------------------------------------------------

def load_bronze_accounts(target_date: str, run_id: str) -> int:
    """
    Load accounts CSV for *target_date* into Bronze.

    Source : s3://credit-card-lake/source/accounts_{target_date}.csv
    Target : s3://credit-card-lake/data/bronze/accounts/date={target_date}/data.parquet

    Returns the number of rows written.
    Raises FileNotFoundError if the source CSV is absent.
    Raises RuntimeError if the target Parquet already exists (idempotency).
    """
    csv_path   = f'{SOURCE_ROOT}/accounts_{target_date}.csv'
    target_dir = f'{BRONZE_ROOT}/accounts/date={target_date}'

    ingested_at = datetime.now(timezone.utc)
    df = read_csv_source(csv_path)
    df = add_audit_columns(df, csv_path, run_id, ingested_at)
    row_count = write_parquet_atomic(df, target_dir)
    assert_row_count(f'{target_dir}/data.parquet', row_count)
    return row_count


# ---------------------------------------------------------------------------
# Transaction codes (non-date-partitioned reference table)
# ---------------------------------------------------------------------------

def load_bronze_transaction_codes(run_id: str) -> int:
    """
    Load transaction_codes CSV into Bronze (no date partition).

    Source : s3://credit-card-lake/source/transaction_codes.csv
    Target : s3://credit-card-lake/data/bronze/transaction_codes/data.parquet

    Returns the number of rows written.
    Raises FileNotFoundError if the source CSV is absent.
    Raises RuntimeError if the target Parquet already exists (idempotency).
    """
    csv_path   = f'{SOURCE_ROOT}/transaction_codes.csv'
    target_dir = f'{BRONZE_ROOT}/transaction_codes'

    ingested_at = datetime.now(timezone.utc)
    df = read_csv_source(csv_path)
    df = add_audit_columns(df, csv_path, run_id, ingested_at)
    row_count = write_parquet_atomic(df, target_dir)
    assert_row_count(f'{target_dir}/data.parquet', row_count)
    return row_count
