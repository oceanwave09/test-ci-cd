CREATE TABLE IF NOT EXISTS TENANT_staging.ecw_allergy (
    row_id VARCHAR,
    apuid VARCHAR,
    record_id VARCHAR,
    patient_id VARCHAR,
    patient_account_no VARCHAR,
    encounter_id VARCHAR,
    encounter_date TIMESTAMP(3) WITH TIME ZONE,
    provider_npi VARCHAR,
    item_id VARCHAR,
    allergies_verified VARCHAR,
    allergen_type VARCHAR,
    allergy_description VARCHAR,
    allergy_reaction_text VARCHAR,
    allergy_status VARCHAR,
    allergy_id VARCHAR,
    modified_date TIMESTAMP(3) WITH TIME ZONE,
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
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/staging/ecw_allergy',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);