CREATE TABLE IF NOT EXISTS baco_analytical.encounter (
    id VARCHAR,
    start_date DATE,
    end_date DATE,
    patient_id VARCHAR,
    provider_id VARCHAR,
    payer_id VARCHAR,
    encounter_class VARCHAR,
    code VARCHAR,
    description VARCHAR,
    base_encounter_cost DOUBLE,
    total_claim_cost DOUBLE,
    payer_coverage DOUBLE,
    reason_code INTEGER,
    reason_description VARCHAR
)
WITH (
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/baco/analytical/encounter',
    PARTITIONED_BY = ARRAY['encounter_class'],
    CHECKPOINT_INTERVAL = 5
);