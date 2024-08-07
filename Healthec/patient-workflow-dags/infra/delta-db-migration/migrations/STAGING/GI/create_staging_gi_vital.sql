CREATE TABLE IF NOT EXISTS TENANT_staging.gi_vital (
    row_id VARCHAR,
    row VARCHAR,
    domain VARCHAR,
    patient_id VARCHAR,
    external_patient_id VARCHAR,
    patient_first_name VARCHAR,
    patient_last_name VARCHAR,
    patient_dob DATE,
    external_practice_id VARCHAR,
    practice_tin VARCHAR,
    vital_code VARCHAR,
    vital_name VARCHAR,
    vital_dictionary_name VARCHAR,
    date_performed TIMESTAMP(3) WITH TIME ZONE,
    date_ordered TIMESTAMP(3) WITH TIME ZONE,
    date_due TIMESTAMP(3) WITH TIME ZONE,
    value VARCHAR,
    provider_npi VARCHAR,
    provider_name VARCHAR,
    record_verified VARCHAR,
    med_unnecessary_reason_code VARCHAR,
    med_unnecessary_reason_name VARCHAR,
    med_unnecessary_reason_dictionary VARCHAR,
    remove_specified_vital_flag VARCHAR,
    vendor VARCHAR,
    source_type VARCHAR,
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
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/staging/gi_vital',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);