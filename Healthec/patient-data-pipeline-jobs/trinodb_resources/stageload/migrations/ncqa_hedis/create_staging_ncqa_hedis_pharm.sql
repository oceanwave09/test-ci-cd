CREATE TABLE IF NOT EXISTS staging.ncqa_hedis_pharm (
    row_id VARCHAR,
    member_id VARCHAR,
    days_supply VARCHAR,
    service_date VARCHAR,
    ndc_drug_code VARCHAR,
    claim_status VARCHAR,
    quantity_dispensed VARCHAR,
    supplemental_data VARCHAR,
    provider_npi VARCHAR,
    pharmacy_npi VARCHAR,
    batch_id VARCHAR,
    source_system VARCHAR,
    file_name VARCHAR,
    status VARCHAR,
    created_user VARCHAR,
    created_ts TIMESTAMP(3) WITH TIME ZONE,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://hec-sandbox-data-pipeline-bucket/delta-tables/staging/ncqa_hedis_pharm',
    PARTITIONED_BY = ARRAY['batch_id'],
    CHECKPOINT_INTERVAL = 5
);
