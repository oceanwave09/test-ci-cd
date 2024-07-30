CREATE TABLE IF NOT EXISTS TENANT_staging.ecw_facility (
    row_id VARCHAR,
    apuid VARCHAR,
    facility_id VARCHAR,
    facility_name VARCHAR,
    address_line_1 VARCHAR,
    address_line_2 VARCHAR,
    address_city VARCHAR,
    address_state VARCHAR,
    address_zip VARCHAR,
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
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/staging/ecw_facility',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);