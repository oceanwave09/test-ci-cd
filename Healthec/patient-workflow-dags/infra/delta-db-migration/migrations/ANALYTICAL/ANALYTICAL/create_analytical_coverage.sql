CREATE TABLE IF NOT EXISTS TENANT_analytical.coverage (
    coverage_fhir_id VARCHAR,
    member_id VARCHAR,
    medicaid_id VARCHAR,
    internal_id VARCHAR,
    status VARCHAR,
    kind VARCHAR,
    type_code VARCHAR,
    type_desc VARCHAR,
    subscriber_id VARCHAR,
    patient_fhir_id VARCHAR,
    dependent VARCHAR,
    relationship_code VARCHAR,
    relationship_desc VARCHAR,
    period_start_date TIMESTAMP(3) WITH TIME ZONE,
    period_end_date TIMESTAMP(3) WITH TIME ZONE,
    payer_fhir_id VARCHAR,
    class VARCHAR,
    class_value VARCHAR,
    network VARCHAR,
    plan_fhir_id VARCHAR,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/analytical/coverage',
    CHECKPOINT_INTERVAL = 5
);