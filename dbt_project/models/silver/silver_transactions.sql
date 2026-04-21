{{
    config(
        materialized='external',
        file_format='parquet',
        location='s3://credit-card-lake/data/silver/transactions/date=' ~ var('target_date') ~ '/data.parquet'
    )
}}

WITH bronze AS (
    SELECT *
    FROM read_parquet(
        's3://credit-card-lake/data/bronze/transactions/date={{ var("target_date") }}/data.parquet'
    )
),

codes AS (
    SELECT transaction_code, debit_credit_indicator, transaction_type
    FROM read_parquet('s3://credit-card-lake/data/bronze/transaction_codes/data.parquet')
)

SELECT
    b.transaction_id,
    b.account_id,
    TRY_CAST(b.transaction_date AS DATE)   AS transaction_date,
    TRY_CAST(b.amount           AS DOUBLE) AS amount,
    b.transaction_code,
    c.transaction_type,
    b.merchant_name,
    b.channel,
    CASE c.debit_credit_indicator
        WHEN 'CR' THEN  TRY_CAST(b.amount AS DOUBLE)
        WHEN 'DR' THEN -TRY_CAST(b.amount AS DOUBLE)
        ELSE NULL
    END AS _signed_amount,
    (c.debit_credit_indicator IS NOT NULL) AS _is_resolvable,
    b._source_file,
    b._ingested_at,
    b._pipeline_run_id
FROM bronze b
LEFT JOIN codes c ON b.transaction_code = c.transaction_code
