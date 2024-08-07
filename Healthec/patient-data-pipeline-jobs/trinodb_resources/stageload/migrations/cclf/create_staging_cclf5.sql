CREATE TABLE IF NOT EXISTS cynchealth_staging.cclf5 (
    row_id VARCHAR,
    cur_clm_uniq_id BIGINT,
    clm_line_num INTEGER,
    bene_mbi_id VARCHAR,
    bene_hic_num VARCHAR,
    clm_type_cd INTEGER,
    clm_from_dt DATE,
    clm_thru_dt DATE,
    rndrg_prvdr_type_cd VARCHAR,
    rndrg_prvdr_fips_st_cd VARCHAR,
    clm_prvdr_spclty_cd VARCHAR,
    clm_fed_type_srvc_cd VARCHAR,
    clm_pos_cd VARCHAR,
    clm_line_from_dt DATE,
    clm_line_thru_dt DATE,
    clm_line_hcpcs_cd VARCHAR,
    clm_line_cvrd_pd_amt VARCHAR,
    clm_line_prmry_pyr_cd VARCHAR,
    clm_line_dgns_cd VARCHAR,
    clm_rndrg_prvdr_tax_num VARCHAR,
    rndrg_prvdr_npi_num VARCHAR,
    clm_carr_pmt_dnl_cd VARCHAR,
    clm_prcsg_ind_cd VARCHAR,
    clm_adjsmt_type_cd VARCHAR,
    clm_efctv_dt DATE,
    clm_idr_ld_dt DATE,
    clm_cntl_num VARCHAR,
    bene_eqtbl_bic_hicn_num VARCHAR,
    clm_line_alowd_chrg_amt VARCHAR,
    clm_line_srvc_unit_qty DOUBLE,
    hcpcs_1_mdfr_cd VARCHAR,
    hcpcs_2_mdfr_cd VARCHAR,
    hcpcs_3_mdfr_cd VARCHAR,
    hcpcs_4_mdfr_cd VARCHAR,
    hcpcs_5_mdfr_cd VARCHAR,
    clm_disp_cd VARCHAR,
    clm_dgns_1_cd VARCHAR,
    clm_dgns_2_cd VARCHAR,
    clm_dgns_3_cd VARCHAR,
    clm_dgns_4_cd VARCHAR,
    clm_dgns_5_cd VARCHAR,
    clm_dgns_6_cd VARCHAR,
    clm_dgns_7_cd VARCHAR,
    clm_dgns_8_cd VARCHAR,
    dgns_prcdr_icd_ind VARCHAR,
    clm_dgns_9_cd VARCHAR,
    clm_dgns_10_cd VARCHAR,
    clm_dgns_11_cd VARCHAR,
    clm_dgns_12_cd VARCHAR,
    hcpcs_betos_cd VARCHAR,
    clm_rndrg_prvdr_npi_num VARCHAR,
    clm_rfrg_prvdr_npi_num VARCHAR,
    clm_cntrctr_num VARCHAR,
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
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/cynchealth/staging/cclf5',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);