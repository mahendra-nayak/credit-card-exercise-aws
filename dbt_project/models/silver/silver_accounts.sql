{{
    config(
        materialized='external',
        file_format='parquet',
        location='s3://credit-card-lake/data/silver/accounts/data.parquet'
    )
}}

SELECT
    account_id,
    customer_name,
    account_status,
    TRY_CAST(credit_limit        AS DOUBLE)  AS credit_limit,
    TRY_CAST(current_balance     AS DOUBLE)  AS current_balance,
    TRY_CAST(open_date           AS DATE)    AS open_date,
    TRY_CAST(billing_cycle_start AS INTEGER) AS billing_cycle_start,
    TRY_CAST(billing_cycle_end   AS INTEGER) AS billing_cycle_end,
    _source_file,
    _ingested_at,
    _pipeline_run_id
FROM read_parquet('s3://credit-card-lake/data/bronze/accounts/**/*.parquet')
