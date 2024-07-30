CREATE TABLE IF NOT EXISTS nucleo_analytical.medication (
    medication_fhir_id VARCHAR,
    external_id VARCHAR,
    code VARCHAR,
    code_desc VARCHAR,
    code_system VARCHAR,
    status VARCHAR,
    form_code VARCHAR,
    form_code_system VARCHAR,
    form_code_desc VARCHAR,
    numerator_value VARCHAR,
    numerator_unit VARCHAR,
    numerator_system VARCHAR,
    numerator_code VARCHAR,
    denominator_value VARCHAR,
    denominator_unit VARCHAR,
    denominator_system VARCHAR,
    denominator_code VARCHAR,
    lot_number VARCHAR,
    expiration_date TIMESTAMP(3) WITH TIME ZONE,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/nucleo/analytical/medication',
    CHECKPOINT_INTERVAL = 5
);