CREATE TABLE IF NOT EXISTS TENANT_analytical.insurance_plan(
    plan_fhir_id VARCHAR,
    internal_id VARCHAR,
    status VARCHAR,
    type_code VARCHAR,
    type_desc VARCHAR,
    name VARCHAR,
    period_start_date TIMESTAMP(3) WITH TIME ZONE,
    period_end_date TIMESTAMP(3) WITH TIME ZONE,
    payer_fhir_id VARCHAR,
    contact_first_name VARCHAR,
    contact_last_name VARCHAR,
    contact_phone VARCHAR,
    contact_email VARCHAR,
    coverage_type_code VARCHAR,
    coverage_type_desc VARCHAR,
    network_fhir_id VARCHAR,
    coverage_benefit_code VARCHAR,
    coverage_benefit_desc VARCHAR,
    coverage_requirement VARCHAR,
    coverage_limit_value VARCHAR,
    coverage_limit_unit VARCHAR,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/analytical/insurance_plan',
    CHECKPOINT_INTERVAL = 5
);
