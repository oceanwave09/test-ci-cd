CREATE TABLE IF NOT EXISTS TENANT_staging.gi_allergy (
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
    allergy_start_date TIMESTAMP(3) WITH TIME ZONE,
    allergy_end_date TIMESTAMP(3) WITH TIME ZONE,
    allergy_reaction_code VARCHAR,
    allergy_reaction_name VARCHAR,
    allergy_reaction_dictionary VARCHAR,
    history_flag VARCHAR,
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
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/staging/gi_allergy',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);
