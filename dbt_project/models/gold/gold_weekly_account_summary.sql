{{
    config(
        materialized='external',
        file_format='parquet',
        location='s3://credit-card-lake/data/gold/.tmp_weekly_account_summary.parquet'
    )
}}

WITH txn AS (
    SELECT *
    FROM read_parquet(
        's3://credit-card-lake/data/silver/transactions/**/*.parquet',
        union_by_name=true
    )
    WHERE _is_resolvable = true
),

weekly AS (
    SELECT
        account_id,
        DATE_TRUNC('week', transaction_date)::DATE                          AS week_start_date,
        (DATE_TRUNC('week', transaction_date) + INTERVAL 6 DAYS)::DATE     AS week_end_date,
        COUNT(*) FILTER (WHERE transaction_type = 'PURCHASE')               AS total_purchases,
        AVG(CASE WHEN transaction_type = 'PURCHASE' THEN _signed_amount END) AS avg_purchase_amount,
        SUM(CASE WHEN transaction_type = 'PAYMENT'  THEN _signed_amount ELSE 0 END) AS total_payments,
        SUM(CASE WHEN transaction_type = 'FEE'      THEN _signed_amount ELSE 0 END) AS total_fees,
        SUM(CASE WHEN transaction_type = 'INTEREST' THEN _signed_amount ELSE 0 END) AS total_interest
    FROM txn
    GROUP BY account_id, DATE_TRUNC('week', transaction_date)
)

SELECT
    w.account_id,
    w.week_start_date,
    w.week_end_date,
    w.total_purchases,
    w.avg_purchase_amount,
    w.total_payments,
    w.total_fees,
    w.total_interest,
    (
        SELECT a.current_balance
        FROM read_parquet(
            's3://credit-card-lake/data/silver/accounts/data.parquet'
        ) a
        WHERE a.account_id = w.account_id
          AND a._record_valid_from <= w.week_end_date
        ORDER BY a._record_valid_from DESC
        LIMIT 1
    )                                           AS closing_balance,
    CURRENT_TIMESTAMP                           AS _computed_at,
    '{{ var("run_id") }}'                       AS _pipeline_run_id
FROM weekly w
