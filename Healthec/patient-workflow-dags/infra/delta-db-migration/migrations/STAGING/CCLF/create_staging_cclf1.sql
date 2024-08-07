CREATE TABLE IF NOT EXISTS TENANT_staging.cclf1 (
    row_id VARCHAR,
    cur_clm_uniq_id INTEGER,
    prvdr_oscar_num VARCHAR,
    bene_mbi_id VARCHAR,
    bene_hic_num VARCHAR,
    clm_type_cd INTEGER,
    clm_from_dt DATE,
    clm_thru_dt DATE,
    clm_bill_fac_type_cd VARCHAR,
    clm_bill_clsfctn_cd VARCHAR,
    prncpl_dgns_cd VARCHAR,
    admtg_dgns_cd VARCHAR,
    clm_mdcr_npmt_rsn_cd VARCHAR,
    clm_pmt_amt DOUBLE,
    clm_nch_prmry_pyr_cd VARCHAR,
    prvdr_fac_fips_st_cd VARCHAR,
    bene_ptnt_stus_cd VARCHAR,
    dgns_drg_cd VARCHAR,
    clm_op_srvc_type_cd VARCHAR,
    fac_prvdr_npi_num VARCHAR,
    oprtg_prvdr_npi_num VARCHAR,
    atndg_prvdr_npi_num VARCHAR,
    othr_prvdr_npi_num VARCHAR,
    clm_adjsmt_type_cd VARCHAR,
    clm_efctv_dt DATE,
    clm_idr_ld_dt DATE,
    bene_eqtbl_bic_hicn_num VARCHAR,
    clm_admsn_type_cd VARCHAR,
    clm_admsn_src_cd VARCHAR,
    clm_bill_freq_cd VARCHAR,
    clm_query_cd VARCHAR,
    dgns_prcdr_icd_ind VARCHAR,
    clm_mdcr_instnl_tot_chrg_amt DOUBLE,
    clm_mdcr_ip_pps_cptl_ime_amt DOUBLE,
    clm_oprtnl_ime_amt DOUBLE,
    clm_mdcr_ip_pps_dsprprtnt_amt DOUBLE,
    clm_hipps_uncompd_care_amt DOUBLE,
    clm_oprtnl_dsprprtnt_amt DOUBLE,
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
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/staging/cclf1',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);