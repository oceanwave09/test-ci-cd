CREATE TABLE IF NOT EXISTS cynchealth_staging.cclf8 (
    row_id VARCHAR,
    bene_mbi_id VARCHAR,
    bene_hic_num VARCHAR,
    bene_fips_state_cd INTEGER,
    bene_fips_cnty_cd INTEGER,
    bene_zip_cd VARCHAR,
    bene_dob VARCHAR,
    bene_sex_cd VARCHAR,
    bene_race_cd VARCHAR,
    bene_age INTEGER,
    bene_mdcr_stus_cd VARCHAR,
    bene_dual_stus_cd VARCHAR,
    bene_death_dt TIMESTAMP(3) WITH TIME ZONE,
    bene_rng_bgn_dt DATE,
    bene_rng_end_dt DATE,
    bene_1st_name VARCHAR,
    bene_midl_name VARCHAR,
    bene_last_name VARCHAR,
    bene_orgnl_entlmt_rsn_cd VARCHAR,
    bene_entlmt_buyin_ind VARCHAR,
    bene_part_a_enrlmt_bgn_dt DATE,
    bene_part_b_enrlmt_bgn_dt DATE,
    bene_line_1_adr VARCHAR,
    bene_line_2_adr VARCHAR,
    bene_line_3_adr VARCHAR,
    bene_line_4_adr VARCHAR,
    bene_line_5_adr VARCHAR,
    bene_line_6_adr VARCHAR,
    geo_zip_plc_name VARCHAR,
    geo_usps_state_cd VARCHAR,
    geo_zip5_cd VARCHAR,
    geo_zip4_cd VARCHAR,
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
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/cynchealth/staging/cclf8',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);