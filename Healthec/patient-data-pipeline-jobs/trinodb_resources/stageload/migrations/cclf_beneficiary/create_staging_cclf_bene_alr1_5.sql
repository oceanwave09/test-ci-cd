CREATE TABLE IF NOT EXISTS nucleo_staging.cclf_bene_alr1_5 (
    row_id VARCHAR,
    bene_mbi_id VARCHAR,
    bene_hic_num VARCHAR,
    bene_1st_name VARCHAR,
    bene_last_name VARCHAR,
    bene_sex_cd VARCHAR,
    bene_brth_dt DATE,
    bene_death_dt DATE,
    plur_r05 INTEGER,
    ab_r01 INTEGER,
    hmo_r03 INTEGER,
    no_us_r02 INTEGER,
    mdm_r04 INTEGER,
    nofnd_r06 INTEGER,
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
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/nucleo/staging/cclf_bene_alr1_5',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);