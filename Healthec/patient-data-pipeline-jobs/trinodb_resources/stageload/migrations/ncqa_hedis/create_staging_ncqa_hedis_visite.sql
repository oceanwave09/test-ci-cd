CREATE TABLE IF NOT EXISTS staging.ncqa_hedis_visite (
    row_id VARCHAR,
    member_id VARCHAR,
    service_date VARCHAR,
    activity_type VARCHAR,
    code_flag VARCHAR,
    end_date VARCHAR,
    encounter_status VARCHAR,
    provider_id VARCHAR,
    diagnosis_code VARCHAR,
    diagnosis_flag VARCHAR,
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
    LOCATION = 's3a://hec-sandbox-data-pipeline-bucket/delta-tables/staging/ncqa_hedis_visite',
    PARTITIONED_BY = ARRAY['batch_id'],
    CHECKPOINT_INTERVAL = 5
);
