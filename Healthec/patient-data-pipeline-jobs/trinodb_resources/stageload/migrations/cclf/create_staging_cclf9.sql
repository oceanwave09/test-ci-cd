CREATE TABLE IF NOT EXISTS cynchealth_staging.cclf9 (
    row_id VARCHAR,
    hicn_mbi_xref_ind VARCHAR,
    crnt_num VARCHAR,
    prvs_num VARCHAR,
    prvs_id_efctv_dt DATE,
    prvs_id_obslt_dt DATE,
    bene_rrb_num VARCHAR,
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
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/cynchealth/staging/cclf9',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);