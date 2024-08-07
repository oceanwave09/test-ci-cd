CREATE TABLE IF NOT EXISTS TENANT_staging.medical_claim (    
    row_id VARCHAR,
    claim_type VARCHAR,
    claim_id VARCHAR,
    previous_claim_id VARCHAR,
    claim_disposition VARCHAR,
    member_id VARCHAR,
    member_mbi VARCHAR,
    member_medicaid_id VARCHAR,
    alternate_member_id VARCHAR,
    subscriber_id VARCHAR,
    member_id_suffix VARCHAR,
    member_ssn VARCHAR,
    member_mrn VARCHAR,
    member_internal_id VARCHAR,
    member_last_name VARCHAR,
    member_first_name VARCHAR,
    member_middle_initial VARCHAR,
    member_dob DATE,
    member_gender VARCHAR,
    member_relationship_code VARCHAR,
    member_address_line_1 VARCHAR,
    member_address_line_2 VARCHAR,
    member_city VARCHAR,
    member_state VARCHAR,
    member_zip VARCHAR,
    insurance_company_name VARCHAR,
    insurance_company_id VARCHAR,
    insurance_group_name VARCHAR,
    insurance_group_id VARCHAR,
    insurance_group_plan_name VARCHAR,
    insurance_group_plan_id VARCHAR,
    primary_payer_code VARCHAR,
    claim_type_of_bill VARCHAR,
    claim_billed_date TIMESTAMP(3) WITH TIME ZONE,
    claim_start_date TIMESTAMP(3) WITH TIME ZONE,
    claim_end_date TIMESTAMP(3) WITH TIME ZONE,
    admission_date TIMESTAMP(3) WITH TIME ZONE,
    admission_type VARCHAR,
    admission_source VARCHAR,
    discharge_date TIMESTAMP(3) WITH TIME ZONE,
    discharge_status VARCHAR,
    admitting_diagnosis VARCHAR,
    primary_diagnosis VARCHAR,
    diagnosis_code_1 VARCHAR,
    diagnosis_code_2 VARCHAR,
    diagnosis_code_3 VARCHAR,
    diagnosis_code_4 VARCHAR,
    principal_procedure_code VARCHAR,
    principal_procedure_desc VARCHAR,
    icd_version VARCHAR,
    drg_code VARCHAR,
    drg_type VARCHAR,
    claim_total_charges DOUBLE,
    claim_adjudication_status VARCHAR,
    claim_total_paid DOUBLE,
    claim_paid_date TIMESTAMP(3) WITH TIME ZONE,
    network_paid_ind VARCHAR,
    claim_deduct_amount DOUBLE,
    claim_copay_amount DOUBLE,
    claim_coinsurance_amount DOUBLE,
    claim_allowed_amount DOUBLE,
    claim_discount_amount DOUBLE,
    claim_patient_paid_amount DOUBLE,
    claim_other_payer_paid DOUBLE,
    line_number VARCHAR,
    line_item_control_number VARCHAR,
    service_line_disposition VARCHAR,
    emergency_indicator VARCHAR,
    service_start_date TIMESTAMP(3) WITH TIME ZONE,
    service_end_date TIMESTAMP(3) WITH TIME ZONE,
    revenue_code VARCHAR,
    cpt_code VARCHAR,
    cpt_code_desc VARCHAR,
    hcpcs_code VARCHAR,
    hcpcs_code_desc VARCHAR,
    service_modifier_1 VARCHAR,
    service_modifier_2 VARCHAR,
    service_modifier_3 VARCHAR,
    service_modifier_4 VARCHAR,
    service_units VARCHAR,
    place_of_service VARCHAR,
    type_of_service VARCHAR,
    service_line_charges DOUBLE,
    line_adjudication_status VARCHAR,
    line_amount_paid DOUBLE,
    service_line_paid_date TIMESTAMP(3) WITH TIME ZONE,
    line_payment_level VARCHAR,
    line_deduct_amount DOUBLE,
    line_copay_amount DOUBLE,
    line_coinsurance_amount DOUBLE,
    line_allowed_amount DOUBLE,
    line_discount_amount DOUBLE,
    line_patient_paid_amount DOUBLE,
    line_other_payer_paid DOUBLE,
    medicare_paid_amount DOUBLE,
    line_allowed_units VARCHAR,
    mco_paid_amount DOUBLE,
    void_sort VARCHAR,
    claim_source_id VARCHAR,
    sort_card VARCHAR,
    attending_npi VARCHAR,
    attending_internal_id VARCHAR,
    attending_last_name VARCHAR,
    attending_first_name VARCHAR,
    reffering_npi VARCHAR,
    reffering_internal_id VARCHAR,
    reffering_last_name VARCHAR,
    reffering_first_name VARCHAR,
    rendering_facility_npi VARCHAR,
    rendering_facility_internal_id VARCHAR,
    rendering_facility_name VARCHAR,
    provider_org_napb_id VARCHAR,
    line_rendering_participation VARCHAR,
    render_prescriber_npi VARCHAR,
    render_prescriber_internal_id VARCHAR,
    render_prescriber_first_name VARCHAR,
    render_prescriber_last_name VARCHAR,
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
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/staging/medical_claim',
    PARTITIONED_BY = ARRAY['file_batch_id'],
    CHECKPOINT_INTERVAL = 5
);