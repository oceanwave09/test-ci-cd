CREATE TABLE IF NOT EXISTS nucleo_staging.cclf_bene_alr1_7 (
    row_id VARCHAR,
    bene_mbi_id VARCHAR,
    b9729 VARCHAR,
    u071 VARCHAR,
    covid19_episode VARCHAR,
    admission_dt DATE,
    discharge_dt DATE,
    covid19_month01 VARCHAR,
    covid19_month02 VARCHAR,
    covid19_month03 VARCHAR,
    covid19_month04 VARCHAR,
    covid19_month05 VARCHAR,
    covid19_month06 VARCHAR,
    covid19_month07 VARCHAR,
    covid19_month08 VARCHAR,
    covid19_month09 VARCHAR,
    covid19_month10 VARCHAR,
    covid19_month11 VARCHAR,
    covid19_month12 VARCHAR,
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
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/nucleo/staging/cclf_bene_alr1_7',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);