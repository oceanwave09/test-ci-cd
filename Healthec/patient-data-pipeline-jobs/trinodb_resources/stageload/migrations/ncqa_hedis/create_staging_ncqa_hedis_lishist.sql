CREATE TABLE IF NOT EXISTS staging.ncqa_hedis_lishist (
    row_id VARCHAR,
    record_type VARCHAR,
    beneficiary_id VARCHAR,
    low_income_period_start_date VARCHAR,
    low_income_period_end_date VARCHAR,
    batch_id VARCHAR,
    source_system VARCHAR,
    file_name VARCHAR,
    status VARCHAR,
    created_user VARCHAR,
    created_ts TIMESTAMP(3) WITH TIME ZONE,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://hec-sandbox-data-pipeline-bucket/delta-tables/staging/ncqa_hedis_lishist',
    PARTITIONED_BY = ARRAY['batch_id'],
    CHECKPOINT_INTERVAL = 5
);