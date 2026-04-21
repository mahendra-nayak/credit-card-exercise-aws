"""
pipeline.py — Credit Card Financial Transactions Lake
Entry point for historical, incremental, and watermark-reset operations.

S3 storage variant: all data paths target s3://credit-card-lake/.
Source CSVs are read from s3://credit-card-lake/source/.
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import uuid
from datetime import datetime, timedelta, timezone

from pipeline.control import read_watermark, write_watermark
from pipeline.run_log import sanitise_error as _sanitise_error, write_run_log_row as _write_run_log_row
from pipeline.s3_utils import s3_copy, s3_delete, s3_exists, get_duckdb_s3_conn

BUCKET_ROOT = 's3://credit-card-lake'

# PID file lives on the local filesystem (process management artifact, not data)
PID_FILE = 'data/pipeline/pipeline.pid'

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
# Gold runner
# ---------------------------------------------------------------------------

def run_gold(run_id: str, pipeline_type: str = 'HISTORICAL') -> None:
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
                started_at=started_at, completed_at=completed_at,
                pipeline_type=pipeline_type,
                status='SUCCESS', records_written=records_written,
            )
            print(f'[gold] {model_name}: SUCCESS ({records_written} rows) -> {canonical_path}')
        else:
            # Clean up staging only — canonical must not be touched
            if s3_exists(staging_path):
                s3_delete(staging_path)

            _write_run_log_row(
                run_id, model_name, 'GOLD',
                started_at=started_at, completed_at=completed_at,
                pipeline_type=pipeline_type,
                status='FAILED',
                error_message=_sanitise_error(result.stderr),
            )
            raise RuntimeError(
                f'{model_name} dbt run failed (exit {result.returncode}):\n'
                + _sanitise_error(result.stderr)
            )


# ---------------------------------------------------------------------------
# S3 canonical paths
# ---------------------------------------------------------------------------

BRONZE_TC_PATH  = f'{BUCKET_ROOT}/data/bronze/transaction_codes/data.parquet'
SILVER_TC_PATH  = f'{BUCKET_ROOT}/data/silver/transaction_codes/data.parquet'
SILVER_ACCT_PATH = f'{BUCKET_ROOT}/data/silver/accounts/data.parquet'


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _bronze_acct_path(date_str: str) -> str:
    return f'{BUCKET_ROOT}/data/bronze/accounts/date={date_str}/data.parquet'


def _bronze_txn_path(date_str: str) -> str:
    return f'{BUCKET_ROOT}/data/bronze/transactions/date={date_str}/data.parquet'


def _silver_txn_path(date_str: str) -> str:
    return f'{BUCKET_ROOT}/data/silver/transactions/date={date_str}/data.parquet'


def _row_count(s3_path: str) -> int:
    conn = get_duckdb_s3_conn()
    return conn.execute(f"SELECT COUNT(*) FROM read_parquet('{s3_path}')").fetchone()[0]


# ---------------------------------------------------------------------------
# Historical pipeline
# ---------------------------------------------------------------------------

def run_historical(start_date: str, end_date: str, run_id: str) -> None:
    """
    Process all dates from start_date to end_date inclusive.

    Resumes from max(start, watermark+1) if a prior watermark exists (RQ-1, INV-35).
    Idempotent no-op if watermark >= end_date (RQ-3).
    Watermark advances only after Gold succeeds for each date (RQ-1, INV-33).
    """
    from pipeline.bronze_loader import (
        load_bronze_accounts, load_bronze_transaction_codes, load_bronze_transactions,
    )
    from pipeline.silver_runner import (
        run_silver_accounts, run_silver_transaction_codes, run_silver_transactions,
    )

    watermark = read_watermark()
    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    end   = datetime.strptime(end_date,   '%Y-%m-%d').date()

    if watermark is not None and watermark >= end:
        print(f'nothing to do: all dates already processed (watermark={watermark})')
        return

    effective_start = max(start, watermark + timedelta(days=1)) if watermark else start

    # --- Bronze TC (once only — INV-24d) ---
    t0 = _now()
    if not s3_exists(BRONZE_TC_PATH):
        rows = load_bronze_transaction_codes(run_id)
        _write_run_log_row(run_id, 'bronze_transaction_codes', 'BRONZE',
                           started_at=t0, completed_at=_now(),
                           pipeline_type='HISTORICAL', status='SUCCESS',
                           records_written=rows)
        print(f'[bronze] transaction_codes: {rows} rows loaded')
    else:
        _write_run_log_row(run_id, 'bronze_transaction_codes', 'BRONZE',
                           started_at=t0, completed_at=_now(),
                           pipeline_type='HISTORICAL', status='SKIPPED')
        print('[bronze] transaction_codes: SKIPPED (already exists)')

    # --- Silver TC (once only — RQ-5) ---
    t0 = _now()
    if not s3_exists(SILVER_TC_PATH):
        run_silver_transaction_codes(run_id)
        rows = _row_count(SILVER_TC_PATH)
        _write_run_log_row(run_id, 'silver_transaction_codes', 'SILVER',
                           started_at=t0, completed_at=_now(),
                           pipeline_type='HISTORICAL', status='SUCCESS',
                           records_written=rows)
        print(f'[silver] transaction_codes: {rows} rows promoted')
    else:
        _write_run_log_row(run_id, 'silver_transaction_codes', 'SILVER',
                           started_at=t0, completed_at=_now(),
                           pipeline_type='HISTORICAL', status='SKIPPED')
        print('[silver] transaction_codes: SKIPPED (already exists)')

    # --- Per-date loop ---
    current = effective_start
    while current <= end:
        date_str = current.strftime('%Y-%m-%d')
        print(f'\n[pipeline] processing {date_str} ...')

        # Bronze accounts
        t0 = _now()
        if not s3_exists(_bronze_acct_path(date_str)):
            rows = load_bronze_accounts(date_str, run_id)
            _write_run_log_row(run_id, 'bronze_accounts', 'BRONZE',
                               started_at=t0, completed_at=_now(),
                               pipeline_type='HISTORICAL', status='SUCCESS',
                               records_written=rows)
            print(f'  [bronze] accounts {date_str}: {rows} rows')
        else:
            _write_run_log_row(run_id, 'bronze_accounts', 'BRONZE',
                               started_at=t0, completed_at=_now(),
                               pipeline_type='HISTORICAL', status='SKIPPED')
            print(f'  [bronze] accounts {date_str}: SKIPPED')

        # Bronze transactions
        t0 = _now()
        if not s3_exists(_bronze_txn_path(date_str)):
            rows = load_bronze_transactions(date_str, run_id)
            _write_run_log_row(run_id, 'bronze_transactions', 'BRONZE',
                               started_at=t0, completed_at=_now(),
                               pipeline_type='HISTORICAL', status='SUCCESS',
                               records_written=rows)
            print(f'  [bronze] transactions {date_str}: {rows} rows')
        else:
            _write_run_log_row(run_id, 'bronze_transactions', 'BRONZE',
                               started_at=t0, completed_at=_now(),
                               pipeline_type='HISTORICAL', status='SKIPPED')
            print(f'  [bronze] transactions {date_str}: SKIPPED')

        # Silver accounts (always re-run — reads all bronze accounts partitions)
        t0 = _now()
        run_silver_accounts(run_id)
        rows = _row_count(SILVER_ACCT_PATH)
        _write_run_log_row(run_id, 'silver_accounts', 'SILVER',
                           started_at=t0, completed_at=_now(),
                           pipeline_type='HISTORICAL', status='SUCCESS',
                           records_written=rows)
        print(f'  [silver] accounts: {rows} rows')

        # Silver transactions (skip if partition already exists — INV-49b)
        t0 = _now()
        if not s3_exists(_silver_txn_path(date_str)):
            run_silver_transactions(date_str, run_id)
            rows = _row_count(_silver_txn_path(date_str))
            _write_run_log_row(run_id, 'silver_transactions', 'SILVER',
                               started_at=t0, completed_at=_now(),
                               pipeline_type='HISTORICAL', status='SUCCESS',
                               records_written=rows)
            print(f'  [silver] transactions {date_str}: {rows} rows')
        else:
            _write_run_log_row(run_id, 'silver_transactions', 'SILVER',
                               started_at=t0, completed_at=_now(),
                               pipeline_type='HISTORICAL', status='SKIPPED')
            print(f'  [silver] transactions {date_str}: SKIPPED')

        # Gold — full recompute every date (RQ-6)
        run_gold(run_id, pipeline_type='HISTORICAL')

        # Watermark advances ONLY after Gold succeeds (RQ-1, INV-33)
        write_watermark(current, run_id)
        print(f'  [watermark] advanced to {current}')

        current += timedelta(days=1)


# ---------------------------------------------------------------------------
# Incremental pipeline
# ---------------------------------------------------------------------------

def run_incremental(run_id: str) -> None:
    """
    Process the single next date after the current watermark.

    No-op (exit 0) if source files for watermark+1 are absent (RQ-4).
    Requires an existing watermark — exits 1 if none found.
    Silver TC is always SKIPPED in incremental (RQ-5).
    """
    from pipeline.bronze_loader import load_bronze_accounts, load_bronze_transactions
    from pipeline.silver_runner import run_silver_accounts, run_silver_transactions

    watermark = read_watermark()
    if watermark is None:
        print('Error: no watermark — run historical pipeline first', file=sys.stderr)
        sys.exit(1)

    target   = watermark + timedelta(days=1)
    date_str = target.strftime('%Y-%m-%d')
    txn_src  = f'{BUCKET_ROOT}/source/transactions_{date_str}.csv'
    acct_src = f'{BUCKET_ROOT}/source/accounts_{date_str}.csv'

    if not s3_exists(txn_src) or not s3_exists(acct_src):
        print(f'no source file for {date_str} — pipeline is current')
        return   # RQ-4: no run log row, no watermark change

    print(f'[incremental] processing {date_str} ...')

    # Silver TC — always SKIPPED in incremental (RQ-5)
    t0 = _now()
    _write_run_log_row(run_id, 'silver_transaction_codes', 'SILVER',
                       started_at=t0, completed_at=_now(),
                       pipeline_type='INCREMENTAL', status='SKIPPED')

    # Bronze accounts
    t0 = _now()
    if not s3_exists(_bronze_acct_path(date_str)):
        rows = load_bronze_accounts(date_str, run_id)
        _write_run_log_row(run_id, 'bronze_accounts', 'BRONZE',
                           started_at=t0, completed_at=_now(),
                           pipeline_type='INCREMENTAL', status='SUCCESS',
                           records_written=rows)
    else:
        _write_run_log_row(run_id, 'bronze_accounts', 'BRONZE',
                           started_at=t0, completed_at=_now(),
                           pipeline_type='INCREMENTAL', status='SKIPPED')

    # Bronze transactions
    t0 = _now()
    if not s3_exists(_bronze_txn_path(date_str)):
        rows = load_bronze_transactions(date_str, run_id)
        _write_run_log_row(run_id, 'bronze_transactions', 'BRONZE',
                           started_at=t0, completed_at=_now(),
                           pipeline_type='INCREMENTAL', status='SUCCESS',
                           records_written=rows)
    else:
        _write_run_log_row(run_id, 'bronze_transactions', 'BRONZE',
                           started_at=t0, completed_at=_now(),
                           pipeline_type='INCREMENTAL', status='SKIPPED')

    # Silver accounts
    t0 = _now()
    run_silver_accounts(run_id)
    rows = _row_count(SILVER_ACCT_PATH)
    _write_run_log_row(run_id, 'silver_accounts', 'SILVER',
                       started_at=t0, completed_at=_now(),
                       pipeline_type='INCREMENTAL', status='SUCCESS',
                       records_written=rows)

    # Silver transactions
    t0 = _now()
    if not s3_exists(_silver_txn_path(date_str)):
        run_silver_transactions(date_str, run_id)
        rows = _row_count(_silver_txn_path(date_str))
        _write_run_log_row(run_id, 'silver_transactions', 'SILVER',
                           started_at=t0, completed_at=_now(),
                           pipeline_type='INCREMENTAL', status='SUCCESS',
                           records_written=rows)
    else:
        _write_run_log_row(run_id, 'silver_transactions', 'SILVER',
                           started_at=t0, completed_at=_now(),
                           pipeline_type='INCREMENTAL', status='SKIPPED')

    # Gold + watermark (RQ-1)
    run_gold(run_id, pipeline_type='INCREMENTAL')
    write_watermark(target, run_id)
    print(f'[watermark] advanced to {target}')


def main() -> None:
    run_id = str(uuid.uuid4())   # INV-43a: must be the first assignment in main()

    # ------------------------------------------------------------------
    # PID file lifecycle (INV-41a–e)
    # ------------------------------------------------------------------
    os.makedirs(os.path.dirname(PID_FILE), exist_ok=True)

    if os.path.exists(PID_FILE):
        pid = int(open(PID_FILE).read().strip())
        try:
            os.kill(pid, 0)                          # signal 0 = existence check
            print(f'Error: pipeline already running (PID {pid})', file=sys.stderr)
            sys.exit(1)
        except ProcessLookupError:
            os.remove(PID_FILE)                      # stale — process is dead
        except PermissionError:
            print(f'Error: pipeline already running (PID {pid})', file=sys.stderr)
            sys.exit(1)
        except OSError:
            # Windows: os.kill(pid, 0) is unreliable; treat as stale
            os.remove(PID_FILE)

    open(PID_FILE, 'w').write(str(os.getpid()))

    def _sigterm(signum, frame):
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
        sys.exit(0)

    signal.signal(signal.SIGTERM, _sigterm)

    # ------------------------------------------------------------------
    # Argument parsing (after PID write — INV-41a)
    # ------------------------------------------------------------------
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

    try:
        if args.historical:
            if not args.start_date or not args.end_date:
                print('Error: --historical requires --start-date and --end-date', file=sys.stderr)
                sys.exit(1)
            run_historical(args.start_date, args.end_date, run_id)
        elif args.incremental:
            run_incremental(run_id)
        elif args.reset_watermark:
            if not args.confirm:
                print('Error: --confirm required for --reset-watermark', file=sys.stderr)
                sys.exit(1)
            prior = read_watermark()
            print(f'Current watermark: {prior or "none"}')
            try:
                target = datetime.strptime(args.reset_watermark, '%Y-%m-%d').date()
            except ValueError:
                print(f"Error: invalid date '{args.reset_watermark}'", file=sys.stderr)
                sys.exit(1)
            write_watermark(target, 'manual-reset')
            print(f'Watermark reset to {target}')
            sys.exit(0)
        else:
            parser.print_help()
            sys.exit(0)
    finally:
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)


if __name__ == '__main__':
    main()
