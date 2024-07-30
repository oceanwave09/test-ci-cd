CREATE TABLE IF NOT EXISTS TENANT_analytical.immunization(
    immunization_fhir_id VARCHAR,
    internal_id VARCHAR,
    status VARCHAR,
    vaccine_code VARCHAR,
    vaccine_desc VARCHAR,
    patient_fhir_id VARCHAR,
    encounter_fhir_id VARCHAR,
    occurrence_date_time TIMESTAMP(3) WITH TIME ZONE,
    facility_fhir_id VARCHAR,
    manufacturer_fhir_id VARCHAR,
    lot_number VARCHAR,
    expiration_date DATE,
    site_code VARCHAR,
    site_desc VARCHAR,
    route_code VARCHAR,
    route_desc VARCHAR,
    dose VARCHAR,
    dose_unit VARCHAR,
    provider_fhir_id VARCHAR,
    reason_fhir_id VARCHAR,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/analytical/immunization',
    CHECKPOINT_INTERVAL = 5
);