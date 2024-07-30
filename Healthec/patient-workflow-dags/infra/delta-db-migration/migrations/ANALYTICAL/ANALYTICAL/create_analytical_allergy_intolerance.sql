CREATE TABLE IF NOT EXISTS TENANT_analytical.allergy_intolerance (
    allergy_intolerance_fhir_id VARCHAR,
    internal_id VARCHAR,
    clinical_status VARCHAR,
    verification_status VARCHAR,
    type VARCHAR,
    category_code VARCHAR,
    category_desc VARCHAR,
    code VARCHAR,
    code_desc VARCHAR,
    patient_fhir_id VARCHAR,
    encounter_fhir_id VARCHAR,
    onset_start_date TIMESTAMP(3) WITH TIME ZONE,
    onset_end_date TIMESTAMP(3) WITH TIME ZONE,
    recorded_date TIMESTAMP(3) WITH TIME ZONE,
    provider_fhir_id VARCHAR,
    reaction_desc VARCHAR,
    substance_code VARCHAR,
    substance_desc VARCHAR,
    manifestation_code VARCHAR,
    manifestation_desc VARCHAR,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/analytical/allergy_intolerance',
    CHECKPOINT_INTERVAL = 5
);