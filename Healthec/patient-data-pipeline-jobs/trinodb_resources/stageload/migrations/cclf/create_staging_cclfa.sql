CREATE TABLE IF NOT EXISTS cynchealth_staging.cclfa (
    row_id VARCHAR,
    cur_clm_uniq_id BIGINT,
    bene_mbi_id VARCHAR,
    bene_hic_num VARCHAR,
    clm_type_cd INTEGER,
    clm_actv_care_from_dt DATE,
    clm_ngaco_pbpmt_sw VARCHAR,
    clm_ngaco_pdschrg_hcbs_sw VARCHAR,
    clm_ngaco_snf_wvr_sw VARCHAR,
    clm_ngaco_tlhlth_sw VARCHAR,
    clm_ngaco_cptatn_sw VARCHAR,
    clm_demo_1st_num VARCHAR,
    clm_demo_2nd_num VARCHAR,
    clm_demo_3rd_num VARCHAR,
    clm_demo_4th_num VARCHAR,
    clm_demo_5th_num VARCHAR,
    clm_pbp_inclsn_amt DOUBLE,
    clm_pbp_rdctn_amt DOUBLE,
    clm_ngaco_cmg_wvr_sw VARCHAR,
    clm_instnl_per_diem_amt DOUBLE,
    clm_mdcr_ip_bene_ddctbl_amt DOUBLE,
    clm_mdcr_coinsrnc_amt DOUBLE,
    clm_blood_lblty_amt DOUBLE,
    clm_instnl_prfnl_amt DOUBLE,
    clm_ncvrd_chrg_amt DOUBLE,
    clm_mdcr_ddctbl_amt DOUBLE,
    clm_rlt_cond_cd VARCHAR,
    clm_oprtnl_outlr_amt DOUBLE,
    clm_mdcr_new_tech_amt DOUBLE,
    clm_islet_isoln_amt DOUBLE,
    clm_sqstrtn_rdctn_amt DOUBLE,
    clm_1_rev_cntr_ansi_rsn_cd VARCHAR,
    clm_1_rev_cntr_ansi_grp_cd VARCHAR,
    clm_mips_pmt_amt DOUBLE,
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
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/cynchealth/staging/cclfa',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);