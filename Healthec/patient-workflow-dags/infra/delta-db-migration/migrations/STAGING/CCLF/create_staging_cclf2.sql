CREATE TABLE IF NOT EXISTS TENANT_staging.cclf2 (
    row_id VARCHAR,
    cur_clm_uniq_id INTEGER,
    clm_line_num INTEGER,
    bene_mbi_id VARCHAR,
    bene_hic_num VARCHAR,
    clm_type_cd INTEGER,
    clm_line_from_dt DATE,
    clm_line_thru_dt DATE,
    clm_line_prod_rev_ctr_cd VARCHAR,
    clm_line_instnl_rev_ctr_dt VARCHAR,
    clm_line_hcpcs_cd VARCHAR,
    bene_eqtbl_bic_hicn_num VARCHAR,
    prvdr_oscar_num VARCHAR,
    clm_from_dt DATE,
    clm_thru_dt DATE,
    clm_line_srvc_unit_qty DOUBLE,
    clm_line_cvrd_pd_amt DOUBLE,
    hcpcs_1_mdfr_cd VARCHAR,
    hcpcs_2_mdfr_cd VARCHAR,
    hcpcs_3_mdfr_cd VARCHAR,
    hcpcs_4_mdfr_cd VARCHAR,
    hcpcs_5_mdfr_cd VARCHAR,
    clm_rev_apc_hipps_cd VARCHAR,
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
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/staging/cclf2',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);