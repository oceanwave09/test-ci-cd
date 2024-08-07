CREATE TABLE IF NOT EXISTS cynchealth_staging.cclfb (
    row_id VARCHAR,
    cur_clm_uniq_id BIGINT,
    clm_line_num INTEGER,
    bene_mbi_id VARCHAR,
    bene_hic_num VARCHAR,
    clm_type_cd INTEGER,
    clm_line_ngaco_pbpmt_sw VARCHAR,
    clm_line_ngaco_pdschrg_hcbs_sw VARCHAR,
    clm_line_ngaco_snf_wvr_sw VARCHAR,
    clm_line_ngaco_tlhlth_sw VARCHAR,
    clm_line_ngaco_cptatn_sw VARCHAR,
    clm_demo_1st_num VARCHAR,
    clm_demo_2nd_num VARCHAR,
    clm_demo_3rd_num VARCHAR,
    clm_demo_4th_num VARCHAR,
    clm_demo_5th_num VARCHAR,
    clm_pbp_inclsn_amt DOUBLE,
    clm_pbp_rdctn_amt DOUBLE,
    clm_ngaco_cmg_wvr_sw VARCHAR,
    clm_mdcr_ddctbl_amt DOUBLE,
    clm_sqstrtn_rdctn_amt DOUBLE,
    clm_line_carr_hpsa_scrcty_cd VARCHAR,
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
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/cynchealth/staging/cclfb',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);