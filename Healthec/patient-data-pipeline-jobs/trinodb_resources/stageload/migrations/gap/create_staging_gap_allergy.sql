CREATE TABLE IF NOT EXISTS cynchealth_staging.gap_allergy (
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
    allergy_start_date VARCHAR,
    allergy_end_date VARCHAR,
    allergy_reaction_code VARCHAR,
    allergy_reaction_name VARCHAR,
    allergy_reaction_dictionary VARCHAR,
    history_flag VARCHAR,
    vendor VARCHAR,
    source_type VARCHAR,
    patient_id VARCHAR,
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
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/cynchealth/staging/gap_allergy',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);