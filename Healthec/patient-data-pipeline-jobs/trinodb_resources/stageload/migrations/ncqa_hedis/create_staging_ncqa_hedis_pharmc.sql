CREATE TABLE IF NOT EXISTS staging.ncqa_hedis_pharmc (
    row_id VARCHAR,
    member_id VARCHAR,
    ordered_date VARCHAR,
    start_date VARCHAR,
    drug_code VARCHAR,
    code_flag VARCHAR,
    frequency VARCHAR,
    dispensed_date VARCHAR,
    end_date VARCHAR,
    active_or_inactive_flag VARCHAR,
    year_of_immunization VARCHAR,
    quantity VARCHAR,
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
    LOCATION = 's3a://hec-sandbox-data-pipeline-bucket/delta-tables/staging/ncqa_hedis_pharmc',
    PARTITIONED_BY = ARRAY['batch_id'],
    CHECKPOINT_INTERVAL = 5
);
