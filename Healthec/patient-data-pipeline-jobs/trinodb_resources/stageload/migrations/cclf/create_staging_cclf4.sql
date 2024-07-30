CREATE TABLE IF NOT EXISTS cynchealth_staging.cclf4 (
    row_id VARCHAR,
    cur_clm_uniq_id BIGINT,
    bene_mbi_id VARCHAR,
    bene_hic_num VARCHAR,
    clm_type_cd INTEGER,
    clm_prod_type_cd VARCHAR,
    clm_val_sqnc_num INTEGER,
    clm_dgns_cd VARCHAR,
    bene_eqtbl_bic_hicn_num VARCHAR,
    prvdr_oscar_num VARCHAR,
    clm_from_dt DATE,
    clm_thru_dt DATE,
    clm_poa_ind VARCHAR,
    dgns_prcdr_icd_ind VARCHAR,
    clm_blg_prvdr_oscar_num VARCHAR,
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
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/cynchealth/staging/cclf4',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);