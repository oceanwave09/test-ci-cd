CREATE TABLE IF NOT EXISTS staging.ncqa_hedis_lab (
    row_id VARCHAR,
    member_id VARCHAR,
    cpt_code VARCHAR,
    loinc_code VARCHAR,
    value VARCHAR,
    date_of_service VARCHAR,
    supplemental_data VARCHAR,
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
    LOCATION = 's3a://hec-sandbox-data-pipeline-bucket/delta-tables/staging/ncqa_hedis_lab',
    PARTITIONED_BY = ARRAY['batch_id'],
    CHECKPOINT_INTERVAL = 5
);