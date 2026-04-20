"""
pipeline/silver_runner.py — Silver layer runner (S3 storage variant).

Two public functions:
  run_silver_static(run_id)              — run silver_accounts + silver_transaction_codes
  run_silver_transactions(target_date, run_id) — run silver_transactions for one date

Both invoke dbt programmatically via subprocess, injecting S3 credentials from
boto3's credential chain into the environment so dbt's DuckDB httpfs profile works
both locally (~/  .aws/credentials) and inside Docker (env vars).

Post-run validation (replacing removed dbt_utils post-hooks):
  - silver_transaction_codes: assert no debit_credit_indicator NOT IN ('DR','CR')
  - silver_transactions: assert no NULL _signed_amount
"""
import os
import subprocess
from pathlib import Path

import boto3

from pipeline.s3_utils import get_duckdb_s3_conn

DBT_PROJECT_DIR = Path(__file__).parent.parent / 'dbt_project'


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _aws_env() -> dict:
    """
    Build an env dict with S3 credentials resolved via boto3's full chain.
    Merges over the current process environment so PATH etc. are preserved.
    """
    session = boto3.Session()
    creds   = session.get_credentials()
    region  = session.region_name or os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')

    env = os.environ.copy()
    env['AWS_DEFAULT_REGION'] = region

    if creds:
        frozen = creds.get_frozen_credentials()
        env['AWS_ACCESS_KEY_ID']     = frozen.access_key
        env['AWS_SECRET_ACCESS_KEY'] = frozen.secret_key
        if frozen.token:
            env['AWS_SESSION_TOKEN'] = frozen.token
        else:
            env.pop('AWS_SESSION_TOKEN', None)

    return env


def _dbt_run(models: list[str], extra_vars: dict | None = None) -> None:
    """
    Run `dbt run --select <models> [--vars <extra_vars>]` inside DBT_PROJECT_DIR.
    Raises RuntimeError on non-zero exit code.
    """
    import shutil
    dbt_exe = shutil.which('dbt') or 'dbt'
    cmd = [
        dbt_exe, 'run',
        '--profiles-dir', str(DBT_PROJECT_DIR),
        '--select', ' '.join(models),
    ]
    if extra_vars:
        import json
        cmd += ['--vars', json.dumps(extra_vars)]

    result = subprocess.run(
        cmd,
        cwd=str(DBT_PROJECT_DIR),
        env=_aws_env(),
        capture_output=False,   # stream output to terminal
    )
    if result.returncode != 0:
        raise RuntimeError(f"dbt run failed (exit {result.returncode}) for models: {models}")


def _validate_silver_transaction_codes() -> None:
    """Assert no invalid debit_credit_indicator values."""
    conn  = get_duckdb_s3_conn()
    count = conn.execute(
        "SELECT COUNT(*) FROM read_parquet("
        "'s3://credit-card-lake/data/silver/transaction_codes/data.parquet')"
        " WHERE debit_credit_indicator NOT IN ('DR','CR')"
    ).fetchone()[0]
    conn.close()
    if count > 0:
        raise AssertionError(
            f"silver_transaction_codes: {count} row(s) with invalid debit_credit_indicator"
        )


def _validate_silver_transactions(target_date: str) -> None:
    """Assert no NULL _signed_amount for the given date partition."""
    path = f"s3://credit-card-lake/data/silver/transactions/date={target_date}/data.parquet"
    conn  = get_duckdb_s3_conn()
    count = conn.execute(
        f"SELECT COUNT(*) FROM read_parquet('{path}') WHERE _signed_amount IS NULL"
    ).fetchone()[0]
    conn.close()
    if count > 0:
        raise AssertionError(
            f"silver_transactions ({target_date}): {count} row(s) with NULL _signed_amount"
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_silver_static(run_id: str) -> None:
    """
    Run dbt Silver models that don't depend on a specific date:
      - silver_accounts
      - silver_transaction_codes

    Validates output via DuckDB httpfs after dbt completes.
    These models overwrite S3 on every run (external materialisation = PUT).
    """
    _dbt_run(['silver_accounts', 'silver_transaction_codes'])
    _validate_silver_transaction_codes()


def run_silver_transactions(target_date: str, run_id: str) -> None:
    """
    Run dbt silver_transactions for *target_date*.

    Passes target_date as a dbt var so the model writes to:
      s3://credit-card-lake/data/silver/transactions/date={target_date}/data.parquet

    Validates that _signed_amount is never NULL after the run.
    """
    _dbt_run(['silver_transactions'], extra_vars={'target_date': target_date})
    _validate_silver_transactions(target_date)
