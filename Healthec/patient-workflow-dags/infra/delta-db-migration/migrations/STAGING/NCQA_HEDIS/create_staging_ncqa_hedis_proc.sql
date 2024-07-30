CREATE TABLE IF NOT EXISTS TENANT_staging.ncqa_hedis_proc (
    row_id VARCHAR,
    member_id VARCHAR,
    service_date VARCHAR,
    procedure_code VARCHAR,
    code_flag VARCHAR,
    end_date VARCHAR,
    service_status VARCHAR,
    attribute VARCHAR,
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
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/staging/ncqa_hedis_proc',
    PARTITIONED_BY = ARRAY['batch_id'],
    CHECKPOINT_INTERVAL = 5
);
