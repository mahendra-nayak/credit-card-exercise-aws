{{
    config(
        materialized='external',
        file_format='parquet',
        location='s3://credit-card-lake/data/silver/transaction_codes/data.parquet'
    )
}}

SELECT
    transaction_code,
    description,
    debit_credit_indicator,
    transaction_type,
    CASE LOWER(affects_balance)
        WHEN 'true'  THEN TRUE
        WHEN 'false' THEN FALSE
        ELSE NULL
    END AS affects_balance,
    _source_file,
    _ingested_at,
    _pipeline_run_id
FROM read_parquet('s3://credit-card-lake/data/bronze/transaction_codes/data.parquet')
