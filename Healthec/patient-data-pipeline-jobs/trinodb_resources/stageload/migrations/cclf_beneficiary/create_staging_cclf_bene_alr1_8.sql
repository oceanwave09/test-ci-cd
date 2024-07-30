CREATE TABLE IF NOT EXISTS nucleo_staging.cclf_bene_alr1_8 (
    row_id VARCHAR,
    participant_tin VARCHAR,
    ccn VARCHAR,
    ccn_type VARCHAR,
    deactivated VARCHAR,
    newly_enrolled VARCHAR,
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
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/nucleo/staging/cclf_bene_alr1_8',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);