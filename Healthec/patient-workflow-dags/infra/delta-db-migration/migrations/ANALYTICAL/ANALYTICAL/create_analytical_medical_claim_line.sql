CREATE TABLE IF NOT EXISTS TENANT_analytical.medical_claim_line (
    claim_fhir_id VARCHAR,
    line_number VARCHAR,
    service_code VARCHAR,
    service_desc VARCHAR,
    revenue_code VARCHAR,
    revenue_desc VARCHAR,
    category_code VARCHAR,
    category_desc VARCHAR,
    serviced_start_date TIMESTAMP(3) WITH TIME ZONE,
    serviced_end_date TIMESTAMP(3) WITH TIME ZONE,
    encounter_fhir_id VARCHAR,
    facility_fhir_id VARCHAR,
    diagnosis_fhir_id VARCHAR,
    procedure_fhir_id VARCHAR,
    quantity DECIMAL,
    unit_price DECIMAL,
    factor DECIMAL,
    net_price DECIMAL,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/analytical/medical_claim_line',
    CHECKPOINT_INTERVAL = 5
);
