CREATE TABLE IF NOT EXISTS TENANT_staging.ncqa_hedis_obs (
    row_id VARCHAR,
    member_id VARCHAR,
    observation_date VARCHAR,
    test VARCHAR,
    test_code_flag VARCHAR,
    value VARCHAR,
    units VARCHAR,
    end_date VARCHAR,
    observation_status VARCHAR,
    result_value_flag VARCHAR,
    type VARCHAR,
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
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/staging/ncqa_hedis_obs',
    PARTITIONED_BY = ARRAY['batch_id'],
    CHECKPOINT_INTERVAL = 5
);
