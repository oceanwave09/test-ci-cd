CREATE TABLE IF NOT EXISTS TENANT_staging.gi_medicalcondition (
    row_id VARCHAR,
    row VARCHAR,
    domain VARCHAR,
    patient_first_name VARCHAR,
    patient_last_name VARCHAR,
    patient_dob DATE,
    external_patient_id VARCHAR,
    external_practice_id VARCHAR,
    code VARCHAR,
    name VARCHAR,
    dictionary VARCHAR,
    start_date TIMESTAMP(3) WITH TIME ZONE,
    end_date TIMESTAMP(3) WITH TIME ZONE,
    resolution_date TIMESTAMP(3) WITH TIME ZONE,
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
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/staging/gi_medicalcondition',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);