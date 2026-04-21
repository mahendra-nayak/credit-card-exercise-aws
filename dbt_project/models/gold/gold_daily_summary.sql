{{
    config(
        materialized='external',
        file_format='parquet',
        location='s3://credit-card-lake/data/gold/.tmp_daily_summary.parquet'
    )
}}

WITH source AS (
    SELECT *
    FROM read_parquet(
        's3://credit-card-lake/data/silver/transactions/**/*.parquet',
        union_by_name=true
    )
),

-- Pass 1: resolvable aggregations
resolvable AS (
    SELECT
        transaction_date,
        COUNT(*)                                                        AS total_transactions,
        SUM(CAST(_signed_amount AS DECIMAL(18,4)))                      AS total_signed_amount,
        COUNT(*) FILTER (WHERE channel = 'ONLINE')                      AS online_transactions,
        COUNT(*) FILTER (WHERE channel = 'IN_STORE')                    AS instore_transactions
    FROM source
    WHERE _is_resolvable = true
    GROUP BY transaction_date
),

-- Pass 2: unresolvable count
unresolvable AS (
    SELECT
        transaction_date,
        COUNT(*) AS unresolvable_count
    FROM source
    WHERE _is_resolvable = false
    GROUP BY transaction_date
),

-- Pass 3: transactions_by_type (dynamic — no hardcoded type names)
by_type_raw AS (
    SELECT
        transaction_date,
        transaction_type,
        COUNT(*) AS type_count
    FROM source
    WHERE _is_resolvable = true
    GROUP BY transaction_date, transaction_type
),

by_type AS (
    SELECT
        transaction_date,
        map_from_entries(
            list(struct_pack(k := transaction_type, v := type_count))
        ) AS transactions_by_type
    FROM by_type_raw
    GROUP BY transaction_date
)

SELECT
    r.transaction_date,
    r.total_transactions,
    r.total_signed_amount,
    bt.transactions_by_type,
    r.online_transactions,
    r.instore_transactions,
    COALESCE(u.unresolvable_count, 0)          AS unresolvable_count,
    CURRENT_TIMESTAMP                           AS _computed_at,
    '{{ var("run_id") }}'                       AS _pipeline_run_id,
    MIN(r.transaction_date) OVER ()             AS _source_period_start,
    MAX(r.transaction_date) OVER ()             AS _source_period_end
FROM resolvable r
LEFT JOIN unresolvable u ON r.transaction_date = u.transaction_date
LEFT JOIN by_type bt      ON r.transaction_date = bt.transaction_date
