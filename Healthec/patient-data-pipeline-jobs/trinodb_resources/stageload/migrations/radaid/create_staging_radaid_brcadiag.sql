CREATE TABLE IF NOT EXISTS nucleo_staging.radaid_brcadiag (
    row_id VARCHAR,
    batch_type VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    mrn VARCHAR,
    diag_date VARCHAR,
    diag_name VARCHAR,
    diag_code VARCHAR,
    diag_result VARCHAR,
    file_batch_id VARCHAR,
    file_name VARCHAR,
    file_source_name VARCHAR,
    file_status VARCHAR,
    created_user VARCHAR,
    created_ts TIMESTAMP(3) WITH TIME ZONE,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/nucleo/staging/radaid_brcadiag',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);