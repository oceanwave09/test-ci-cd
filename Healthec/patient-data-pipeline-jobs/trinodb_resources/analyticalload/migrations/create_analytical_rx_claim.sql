CREATE TABLE IF NOT EXISTS nucleo_analytical.rx_claim (
    claim_fhir_id VARCHAR,
    claim_number VARCHAR,
    type_code VARCHAR,
    type_system VARCHAR,
    type_desc VARCHAR,
    use VARCHAR,
    patient_fhir_id VARCHAR,
    billable_start_date TIMESTAMP(3) WITH TIME ZONE,
    billable_end_date TIMESTAMP(3) WITH TIME ZONE,
    created TIMESTAMP(3) WITH TIME ZONE,
    enterer_fhir_id VARCHAR,
    payer_fhir_id VARCHAR,
    provider_fhir_id VARCHAR,
    prescription_fhir_id VARCHAR,
    payee_type_code VARCHAR,
    payee_type_system VARCHAR,
    payee_type_desc VARCHAR,
    payee_fhir_id VARCHAR,
    facility_fhir_id VARCHAR,
    coverage_fhir_id VARCHAR,
    total_amount VARCHAR,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/nucleo/analytical/rx_claim',
    CHECKPOINT_INTERVAL = 5
);