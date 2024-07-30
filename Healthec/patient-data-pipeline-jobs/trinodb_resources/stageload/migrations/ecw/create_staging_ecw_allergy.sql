CREATE TABLE IF NOT EXISTS cynchealth_staging.ecw_allergy (
    row_id VARCHAR,
    apuid VARCHAR,
    record_id VARCHAR,
    patient_id VARCHAR,
    patient_account_no VARCHAR,
    encounter_id VARCHAR,
    encounter_date VARCHAR,
    provider_npi VARCHAR,
    item_id VARCHAR,
    allergies_verified VARCHAR,
    allergen_type VARCHAR,
    allergy_description VARCHAR,
    allergy_reaction_text VARCHAR,
    allergy_status VARCHAR,
    allergy_id VARCHAR,
    modified_date VARCHAR,
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
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/cynchealth/staging/ecw_allergy',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);