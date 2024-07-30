CREATE TABLE IF NOT EXISTS nucleo_staging.radaid_cxcaproc (
    row_id VARCHAR,
    batch_type VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    mrn VARCHAR,
    proc_date VARCHAR,
    proc_name VARCHAR,
    proc_code VARCHAR,
    proc_result VARCHAR,
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
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/nucleo/staging/radaid_cxcaproc',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);