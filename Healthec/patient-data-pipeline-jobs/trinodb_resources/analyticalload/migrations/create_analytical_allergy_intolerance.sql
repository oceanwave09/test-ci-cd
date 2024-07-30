CREATE TABLE IF NOT EXISTS nucleo_analytical.allergy_intolerance (
    allergy_intolerance_fhir_id VARCHAR,
    external_id VARCHAR,
    clinical_status VARCHAR,
    clinical_system VARCHAR,
    clinical_desc VARCHAR,
    verification_status VARCHAR,
    verification_system VARCHAR,
    verification_desc VARCHAR,
    type VARCHAR,
    category VARCHAR,
    criticality VARCHAR,
    code VARCHAR,
    code_desc VARCHAR,
    code_system VARCHAR,
    patient_fhir_id VARCHAR,
    encounter_fhir_id VARCHAR,
    onset_start_date TIMESTAMP(3) WITH TIME ZONE,
    onset_end_date TIMESTAMP(3) WITH TIME ZONE,
    recorded_date TIMESTAMP(3) WITH TIME ZONE,
    provider_fhir_id VARCHAR,
    last_occurance_date VARCHAR,
    reaction_desc VARCHAR,
    manifestation_code VARCHAR,
    manifestation_code_system VARCHAR,
    manifestation_desc VARCHAR,
    onset TIMESTAMP(3) WITH TIME ZONE,
    severity VARCHAR,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/nucleo/analytical/allergy_intolerance',
    CHECKPOINT_INTERVAL = 5
);