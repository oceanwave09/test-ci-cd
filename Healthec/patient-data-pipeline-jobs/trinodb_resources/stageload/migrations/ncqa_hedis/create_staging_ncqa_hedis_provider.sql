CREATE TABLE IF NOT EXISTS staging.ncqa_hedis_provider (
    row_id VARCHAR,
    provider_id VARCHAR,
    pcp VARCHAR,
    obgyn VARCHAR,
    mh_provider VARCHAR,
    eye_care_provider VARCHAR,
    dentist VARCHAR,
    nephrologist VARCHAR,
    anesthesiologist VARCHAR,
    npr_provider VARCHAR,
    pas_provider VARCHAR,
    provider_prescribing_privileges VARCHAR,
    clinical_pharmacist VARCHAR,
    hospital VARCHAR,
    snf VARCHAR,
    surgeon VARCHAR,
    registered_nurse VARCHAR,
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
    LOCATION = 's3a://hec-sandbox-data-pipeline-bucket/delta-tables/staging/ncqa_hedis_provider',
    PARTITIONED_BY = ARRAY['batch_id'],
    CHECKPOINT_INTERVAL = 5
);
