CREATE TABLE IF NOT EXISTS nucleo_analytical.measure (
    patient_fhir_id VARCHAR,
    practice_fhir_id VARCHAR,
    provider_fhir_id ARRAY(VARCHAR),
    coverage_fhir_id VARCHAR,
    payer_fhir_id VARCHAR,
    plan_fhir_id VARCHAR,
    measure_id VARCHAR,
    group_key INTEGER,
    numerator BOOLEAN,
    numerator_exclusion BOOLEAN,
    numerator_exception BOOLEAN,
    denominator_exclusion BOOLEAN,
    denominator_exception BOOLEAN,
    denominator BOOLEAN,
    initial_population BOOLEAN,
    from_date DATE,
    to_date DATE,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/nucleo/analytical/measure',
    CHECKPOINT_INTERVAL = 5
);