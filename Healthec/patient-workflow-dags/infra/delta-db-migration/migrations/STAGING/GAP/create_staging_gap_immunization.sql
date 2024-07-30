CREATE TABLE IF NOT EXISTS TENANT_staging.gap_immunization (
    row_id VARCHAR,
    row VARCHAR,
    domain VARCHAR,
    patient_id VARCHAR,
    external_patient_id VARCHAR,
    patient_first_name VARCHAR,
    patient_last_name VARCHAR,
    patient_dob DATE,
    external_practice_id VARCHAR,
    practice_tin VARCHAR,
    code VARCHAR,
    name VARCHAR,
    dictionary_name VARCHAR,
    date_performed TIMESTAMP(3) WITH TIME ZONE,
    date_ordered TIMESTAMP(3) WITH TIME ZONE,
    date_due TIMESTAMP(3) WITH TIME ZONE,
    refused VARCHAR,
    med_unnecessary_reason_code VARCHAR,
    med_unnecessary_reason_name VARCHAR,
    med_unnecessary_reason_dictionary VARCHAR,
    provider_npi VARCHAR,
    provider_name VARCHAR,
    administered_amount VARCHAR,
    administered_unit VARCHAR,
    vaccine_lot_number VARCHAR,
    immunization_manufacture_code VARCHAR,
    vendor VARCHAR,
    source_type VARCHAR,
    remove_specified_immunization_flag VARCHAR,
    vfc_eligibility_code VARCHAR,
    vfc_eligibility_dict_name VARCHAR,
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
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/staging/gap_immunization',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);