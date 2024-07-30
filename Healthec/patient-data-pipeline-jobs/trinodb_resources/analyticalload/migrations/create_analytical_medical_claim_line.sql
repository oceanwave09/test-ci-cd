CREATE TABLE IF NOT EXISTS nucleo_analytical.medical_claim_line (
    claim_fhir_id VARCHAR,
    line_number VARCHAR,
    service_code VARCHAR,
    service_system VARCHAR,
    service_desc VARCHAR,
    revenue_code VARCHAR,
    revenue_system VARCHAR,
    revenue_desc VARCHAR,
    category_code VARCHAR,
    category_system VARCHAR,
    category_desc VARCHAR,
    serviced_start_date TIMESTAMP(3) WITH TIME ZONE,
    serviced_end_date TIMESTAMP(3) WITH TIME ZONE,
    encounter_fhir_id VARCHAR,
    facility_fhir_id VARCHAR,
    diagnosis_sequence VARCHAR,
    diagnosis_fhir_id VARCHAR,
    procedure_sequence VARCHAR,
    procedure_fhir_id VARCHAR,
    quantity VARCHAR,
    unit_price VARCHAR,
    unit_price_currency VARCHAR,
    factor VARCHAR,
    net_price VARCHAR,
    net_price_currency VARCHAR,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/nucleo/analytical/medical_claim_line',
    CHECKPOINT_INTERVAL = 5
);
