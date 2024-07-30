CREATE TABLE IF NOT EXISTS TENANT_staging.ecw_orderandresult (
    row_id VARCHAR,
    apuid VARCHAR,
    record_id VARCHAR,
    order_type VARCHAR,
    item_id VARCHAR,
    lonic VARCHAR,
    cpt VARCHAR,
    order_id VARCHAR,
    patient_id VARCHAR,
    patient_account_no VARCHAR,
    provider_id VARCHAR,
    encounter_id VARCHAR,
    provider_npi VARCHAR,
    order_date TIMESTAMP(3) WITH TIME ZONE,
    order_description VARCHAR,
    result_description VARCHAR,
    collection_date TIMESTAMP(3) WITH TIME ZONE,
    result_date TIMESTAMP(3) WITH TIME ZONE,
    result_text VARCHAR,
    lab_reviewed_flag VARCHAR,
    reviewed_by_id VARCHAR,
    result_numeric VARCHAR,
    ref_range VARCHAR,
    hl_units VARCHAR,
    result_modify_timestamp TIMESTAMP(3) WITH TIME ZONE,
    lab_company_id VARCHAR,
    lab_company_name VARCHAR,
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
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/staging/ecw_orderandresult',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);