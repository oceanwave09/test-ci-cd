CREATE TABLE IF NOT EXISTS nucleo_staging.radaid_patient (
    row_id VARCHAR,
    batch_type VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    mrn VARCHAR,
    year_of_birth VARCHAR,
    race VARCHAR,
    ethnicity VARCHAR,
    language VARCHAR,
    years_in_us VARCHAR,
    total_income VARCHAR,
    insurance_type VARCHAR,
    high_deductible_insurance VARCHAR,
    marital_status VARCHAR,
    family_size VARCHAR,
    family_brca_history VARCHAR,
    history_brca VARCHAR,
    brca VARCHAR,
    file_batch_id VARCHAR,
    file_name VARCHAR,
    file_source_name VARCHAR,
    file_status VARCHAR,
    created_user VARCHAR,
    created_ts TIMESTAMP(3) WITH TIME ZONE,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/nucleo/staging/radaid_patient',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);