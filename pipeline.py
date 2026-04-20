"""
pipeline.py — Credit Card Financial Transactions Lake
Entry point for historical, incremental, and watermark-reset operations.

S3 storage variant: all data paths target s3://credit-card-lake/.
Source CSVs are read from s3://credit-card-lake/source/.
"""
import argparse
import sys
import uuid


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
