{{
    config(
        materialized='external',
        file_format='parquet',
        location='s3://credit-card-lake/data/silver/accounts/data.parquet'
    )
}}

WITH bronze AS (
    SELECT *,
        TRY_CAST(regexp_extract(_source_file, '(\d{4}-\d{2}-\d{2})') AS DATE) AS _record_valid_from,
        ROW_NUMBER() OVER (
            PARTITION BY account_id,
                         TRY_CAST(regexp_extract(_source_file, '(\d{4}-\d{2}-\d{2})') AS DATE)
            ORDER BY _source_row_number DESC
        ) AS _rn
    FROM read_parquet('s3://credit-card-lake/data/bronze/accounts/**/*.parquet', union_by_name=true)
)

SELECT
    account_id,
    customer_name,
    account_status,
    TRY_CAST(credit_limit        AS DOUBLE)  AS credit_limit,
    TRY_CAST(current_balance     AS DOUBLE)  AS current_balance,
    TRY_CAST(open_date           AS DATE)    AS open_date,
    TRY_CAST(billing_cycle_start AS INTEGER) AS billing_cycle_start,
    TRY_CAST(billing_cycle_end   AS INTEGER) AS billing_cycle_end,
    _record_valid_from,
    _source_file,
    _ingested_at,
    _pipeline_run_id
FROM bronze
WHERE _rn = 1
