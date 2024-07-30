CREATE TABLE IF NOT EXISTS cynchealth_staging.gap_medicalcondition (
    row_id VARCHAR,
    row VARCHAR,
    domain VARCHAR,
    patient_first_name VARCHAR,
    patient_last_name VARCHAR,
    patient_dob VARCHAR,
    external_patient_id VARCHAR,
    external_practice_id VARCHAR,
    code VARCHAR,
    name VARCHAR,
    dictionary_name VARCHAR,
    start_date VARCHAR,
    end_date VARCHAR,
    resolution_date VARCHAR,
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
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/cynchealth/staging/gap_medicalcondition',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);