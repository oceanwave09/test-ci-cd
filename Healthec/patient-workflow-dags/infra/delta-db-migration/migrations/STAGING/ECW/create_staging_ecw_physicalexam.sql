CREATE TABLE IF NOT EXISTS TENANT_staging.ecw_physicalexam (
    row_id VARCHAR,
    apuid VARCHAR,
    record_id VARCHAR,
    patient_id VARCHAR,
    patient_account_no VARCHAR,
    encounter_id VARCHAR,
    encounter_date TIMESTAMP(3) WITH TIME ZONE,
    provider_npi VARCHAR,
    ecw_struct_id VARCHAR,
    question VARCHAR,
    question_id VARCHAR,
    answers VARCHAR,
    answers_id VARCHAR,
    item_name VARCHAR,
    item_id VARCHAR,
    category_name VARCHAR,
    category_id VARCHAR,
    main_category_name VARCHAR,
    main_category_id VARCHAR,
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
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/staging/ecw_physicalexam',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);