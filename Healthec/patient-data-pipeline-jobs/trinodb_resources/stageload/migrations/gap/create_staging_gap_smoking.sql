CREATE TABLE IF NOT EXISTS cynchealth_staging.gap_smoking (
    row_id VARCHAR,
    row VARCHAR,
    domain VARCHAR,
    patient_first_name VARCHAR,
    patient_last_name VARCHAR,
    patient_dob VARCHAR,
    external_patient_id VARCHAR,
	external_practice_id VARCHAR,
    smoking_status_code VARCHAR,
    smoking_status_name VARCHAR,
    smoking_dictionary_name VARCHAR,
    start_date VARCHAR,
    end_date VARCHAR,
    last_confirmed_date VARCHAR,
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
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/cynchealth/staging/gap_smoking',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);