CREATE TABLE IF NOT EXISTS staging.ncqa_hedis_visit (
    row_id VARCHAR,
    member_id VARCHAR,
    date_of_service VARCHAR,
    admission_date VARCHAR,
    discharge_date VARCHAR,
    cpt VARCHAR,
    cpt_modifier_1 VARCHAR,
    cpt_modifier_2 VARCHAR,
    hcpcs VARCHAR,
    cpt_ii VARCHAR,
    cpt_ii_modifier VARCHAR,
    principal_icd_diagnosis VARCHAR,
    icd_diagnosis_2 VARCHAR,
    icd_diagnosis_3 VARCHAR,
    icd_diagnosis_4 VARCHAR,
    icd_diagnosis_5 VARCHAR,
    icd_diagnosis_6 VARCHAR,
    icd_diagnosis_7 VARCHAR,
    icd_diagnosis_8 VARCHAR,
    icd_diagnosis_9 VARCHAR,
    icd_diagnosis_10 VARCHAR,
    icd_diagnosis_11 VARCHAR,
    icd_diagnosis_12 VARCHAR,
    icd_diagnosis_13 VARCHAR,
    icd_diagnosis_14 VARCHAR,
    icd_diagnosis_15 VARCHAR,
    icd_diagnosis_16 VARCHAR,
    icd_diagnosis_17 VARCHAR,
    icd_diagnosis_18 VARCHAR,
    icd_diagnosis_19 VARCHAR,
    icd_diagnosis_20 VARCHAR,
    principal_icd_procedure VARCHAR,
    icd_procedure_2 VARCHAR,
    icd_procedure_3 VARCHAR,
    icd_procedure_4 VARCHAR,
    icd_procedure_5 VARCHAR,
    icd_procedure_6 VARCHAR,
    icd_identifier VARCHAR,
    discharge_status VARCHAR,
    ub_revenue VARCHAR,
    ub_type_of_bill VARCHAR,
    cms_place_of_service VARCHAR,
    claim_status VARCHAR,
    provider_id VARCHAR,
    supplemental_data VARCHAR,
    claim_id VARCHAR,
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
    LOCATION = 's3a://hec-sandbox-data-pipeline-bucket/delta-tables/staging/ncqa_hedis_visit',
    PARTITIONED_BY = ARRAY['batch_id'],
    CHECKPOINT_INTERVAL = 5
);
