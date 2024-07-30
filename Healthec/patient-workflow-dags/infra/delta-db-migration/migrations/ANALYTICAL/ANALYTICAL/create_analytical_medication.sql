CREATE TABLE IF NOT EXISTS TENANT_analytical.medication (
    medication_fhir_id VARCHAR,
    internal_id VARCHAR,
    code VARCHAR,
    code_desc VARCHAR,
    status VARCHAR,
    manufacturer_fhir_id VARCHAR,
    form VARCHAR,
    lot_number VARCHAR,
    expiration_date TIMESTAMP(3) WITH TIME ZONE,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/analytical/medication',
    CHECKPOINT_INTERVAL = 5
);