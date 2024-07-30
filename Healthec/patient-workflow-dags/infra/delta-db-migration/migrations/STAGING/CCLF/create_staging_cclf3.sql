CREATE TABLE IF NOT EXISTS TENANT_staging.cclf3 (
    row_id VARCHAR,
    cur_clm_uniq_id INTEGER,
    bene_mbi_id VARCHAR,
    bene_hic_num VARCHAR,
    clm_type_cd INTEGER,
    clm_val_sqnc_num INTEGER,
    clm_prcdr_cd VARCHAR,
    clm_prcdr_prfrm_dt TIMESTAMP(3) WITH TIME ZONE,
    bene_eqtbl_bic_hicn_num VARCHAR,
    prvdr_oscar_num VARCHAR,
    clm_from_dt DATE,
    clm_thru_dt DATE,
    dgns_prcdr_icd_ind VARCHAR,
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
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/staging/cclf3',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);