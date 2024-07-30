CREATE TABLE IF NOT EXISTS nucleo_analytical.insurance_plan(
    plan_fhir_id VARCHAR,
    external_id VARCHAR,
    status VARCHAR,
    type_system VARCHAR,
    type_code VARCHAR,
    type_desc VARCHAR,
    alias VARCHAR,
    name VARCHAR,
    period_start_date TIMESTAMP(3) WITH TIME ZONE,
    period_end_date TIMESTAMP(3) WITH TIME ZONE,
    payer_fhir_id VARCHAR,
    coverage_type_system VARCHAR,
    coverage_type_code VARCHAR,
    coverage_type_desc VARCHAR,
    network_fhir_id VARCHAR,
    coverage_benefit_system VARCHAR,
    coverage_benefit_code VARCHAR,
    coverage_benefit_desc VARCHAR,
    coverage_requirement VARCHAR,
    coverage_limit_value VARCHAR,
    coverage_limit_unit VARCHAR,
    coverage_administered_by VARCHAR,
    coverage_area VARCHAR,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/nucleo/analytical/insurance_plan',
    CHECKPOINT_INTERVAL = 5
);