CREATE TABLE IF NOT EXISTS nucleo_staging.cclf_bene_alr1_9 (
    row_id VARCHAR,
    bene_mbi_id VARCHAR,
    adi_natrank INTEGER,
    bene_lis_status INTEGER,
    bene_dual_status INTEGER,
    bene_psnyrs_lis_dual INTEGER,
    bene_psnyrs INTEGER,
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
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/nucleo/staging/cclf_bene_alr1_9',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);