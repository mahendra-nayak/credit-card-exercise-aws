"""
pipeline.py — Credit Card Financial Transactions Lake
Entry point for historical, incremental, and watermark-reset operations.

S3 storage variant: all data paths target s3://credit-card-lake/.
Source CSVs are read from s3://credit-card-lake/source/.
"""
import argparse
import json
import subprocess
import sys
import uuid
from datetime import datetime, timezone

from pipeline.s3_utils import s3_copy, s3_delete, s3_exists, get_duckdb_s3_conn

BUCKET_ROOT = 's3://credit-card-lake'

# Gold staging → canonical paths
GOLD_STAGING = {
    'gold_daily_summary': (
        f'{BUCKET_ROOT}/data/gold/.tmp_daily_summary.parquet',
        f'{BUCKET_ROOT}/data/gold/daily_summary/data.parquet',
    ),
    'gold_weekly_account_summary': (
        f'{BUCKET_ROOT}/data/gold/.tmp_weekly_account_summary.parquet',
        f'{BUCKET_ROOT}/data/gold/weekly_account_summary/data.parquet',
    ),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sanitise_error(text: str) -> str:
    """Return the last 500 chars of dbt stderr — enough context, not too long."""
    return (text or '').strip()[-500:]


def _write_run_log_row(run_id: str, model: str, layer: str,
                       started_at: datetime, completed_at: datetime,
                       status: str, records_written: int = 0,
                       error_message: str = '') -> None:
    """
    Stub — will be replaced by full S3 run log writer in Task 5.2.
    For now prints to stdout so the audit trail is visible in container logs.
    """
    print(
        f'[run_log] run_id={run_id} model={model} layer={layer} '
        f'status={status} records={records_written} '
        f'started={started_at.isoformat()} completed={completed_at.isoformat()}'
        + (f' error={error_message}' if error_message else '')
    )


# ---------------------------------------------------------------------------
# Gold runner
# ---------------------------------------------------------------------------

def run_gold(run_id: str) -> None:
    """
    Invoke both Gold dbt models and atomically rename each staging file
    to its canonical S3 path.

    For each model:
      1. Run dbt --select <model_name> --vars {run_id: ...}
      2. On success: s3_copy(staging → canonical) then s3_delete(staging)
      3. On failure: s3_delete(staging only) — canonical is never touched
      4. Write a run log row for each model
    """
    for model_name, (staging_path, canonical_path) in GOLD_STAGING.items():
        started_at = datetime.now(timezone.utc)

        result = subprocess.run(
            [
                'dbt', 'run',
                '--select', model_name,
                '--vars', json.dumps({'run_id': run_id}),
                '--profiles-dir', 'dbt_project',
                '--project-dir', 'dbt_project',
            ],
            capture_output=True,
            text=True,
        )
        completed_at = datetime.now(timezone.utc)

        if result.returncode == 0 and s3_exists(staging_path):
            # Atomic rename: copy staging → canonical, then delete staging
            s3_copy(staging_path, canonical_path)
            s3_delete(staging_path)

            conn = get_duckdb_s3_conn()
            records_written = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{canonical_path}')"
            ).fetchone()[0]

            _write_run_log_row(
                run_id, model_name, 'GOLD',
                started_at, completed_at,
                status='SUCCESS', records_written=records_written,
            )
            print(f'[gold] {model_name}: SUCCESS ({records_written} rows) -> {canonical_path}')
        else:
            # Clean up staging only — canonical must not be touched
            if s3_exists(staging_path):
                s3_delete(staging_path)

            _write_run_log_row(
                run_id, model_name, 'GOLD',
                started_at, completed_at,
                status='FAILED',
                error_message=_sanitise_error(result.stderr),
            )
            raise RuntimeError(
                f'{model_name} dbt run failed (exit {result.returncode}):\n'
                + _sanitise_error(result.stderr)
            )


def main() -> None:
    run_id = str(uuid.uuid4())

    parser = argparse.ArgumentParser(
        prog='pipeline.py',
        description='Credit card lake pipeline (S3 storage variant)',
    )
    group = parser.add_mutually_exclusive_group()

    group.add_argument('--historical', action='store_true',
                       help='Run historical pipeline over a date range')
    parser.add_argument('--start-date', metavar='YYYY-MM-DD',
                        help='Inclusive start date for --historical')
    parser.add_argument('--end-date', metavar='YYYY-MM-DD',
                        help='Inclusive end date for --historical')

    group.add_argument('--incremental', action='store_true',
                       help='Run incremental pipeline for the next unprocessed date')

    group.add_argument('--reset-watermark', metavar='YYYY-MM-DD',
                       help='Reset the watermark to a specific date')
    parser.add_argument('--confirm', action='store_true',
                        help='Required confirmation flag for --reset-watermark')

    args = parser.parse_args()

    if args.historical:
        print(f'[historical] run_id={run_id} (not yet implemented)')
        sys.exit(0)
    elif args.incremental:
        print(f'[incremental] run_id={run_id} (not yet implemented)')
        sys.exit(0)
    elif args.reset_watermark:
        print(f'[reset-watermark] run_id={run_id} (not yet implemented)')
        sys.exit(0)
    else:
        parser.print_help()
        sys.exit(0)


if __name__ == '__main__':
    main()
