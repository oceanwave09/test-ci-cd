CREATE TABLE IF NOT EXISTS TENANT_staging.ncqa_hedis_mmdf (
    row_id VARCHAR,
    run_date VARCHAR,
    payment_date VARCHAR,
    beneficiary_id VARCHAR,
    hospice VARCHAR,
    long_term_institutionalized_flag VARCHAR,
    unknown_field_1 VARCHAR,
    original_reason_for_entitlement_code VARCHAR,
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
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/staging/ncqa_hedis_mmdf',
    PARTITIONED_BY = ARRAY['batch_id'],
    CHECKPOINT_INTERVAL = 5
);