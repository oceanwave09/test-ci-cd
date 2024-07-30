CREATE TABLE IF NOT EXISTS TENANT_staging.ncqa_hedis_memberen (
    row_id VARCHAR,
    member_id VARCHAR,
    start_date VARCHAR,
    disenrollment_date VARCHAR,
    dental_benefit VARCHAR,
    drug_benefit VARCHAR,
    mental_health_benefit_inpatient VARCHAR,
    mental_health_benefit_intensive_outpatient VARCHAR,
    mental_health_benefit_outpatient_ed VARCHAR,
    chemdep_benefit_inpatient VARCHAR,
    chemdep_benefit_intensive_outpatient VARCHAR,
    chemdep_benefit_outpatient_ed VARCHAR,
    payer VARCHAR,
    health_plan_employee_flag VARCHAR,
    indicator VARCHAR,
    batch_id VARCHAR,
    source_system VARCHAR,
    file_name VARCHAR,
    status VARCHAR,
    created_user VARCHAR,
    created_ts TIMESTAMP(3) WITH TIME ZONE,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/staging/ncqa_hedis_memberen',
    PARTITIONED_BY = ARRAY['batch_id'],
    CHECKPOINT_INTERVAL = 5
);