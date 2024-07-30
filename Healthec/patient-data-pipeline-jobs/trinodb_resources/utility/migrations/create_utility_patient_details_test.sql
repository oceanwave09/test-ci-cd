CREATE TABLE IF NOT EXISTS nucleo_analytical.patient_details_test (
    dob VARCHAR,
    is_test_patient VARCHAR,
    healthrecordkey VARCHAR,
    title VARCHAR,
    lastname VARCHAR,
    firstname VARCHAR,
    patient_name VARCHAR,
    sex VARCHAR,
    gender VARCHAR,
    age VARCHAR,
    dob_order VARCHAR,
    city VARCHAR,
    zip_code VARCHAR,
    state VARCHAR,
    phone_home VARCHAR,
    phone_work VARCHAR,
    phone_mobile VARCHAR,
    fax VARCHAR,
    email VARCHAR,
    county VARCHAR,
    beneficiary_status VARCHAR,
    consent_status VARCHAR,
    medicare_id VARCHAR,
    ssn VARCHAR,
    empi_mrn VARCHAR,
    empi_mbi VARCHAR,
    empi_medicare VARCHAR,
    risk_score VARCHAR,
    risk_level VARCHAR,
    ethnicity_code VARCHAR,
    ethnicity VARCHAR,
    race VARCHAR,
    mothers_maiden_name VARCHAR,
    nationality VARCHAR,
    birth_place VARCHAR,
    primary_language VARCHAR,
    secondary_language VARCHAR,
    death_indicator VARCHAR,
    death_date VARCHAR,
    account_name VARCHAR,
    provider_name VARCHAR,
    region_name VARCHAR,
    chronic_conditions VARCHAR,
    log_date VARCHAR,
    care_giver_phone VARCHAR,
    referral_type VARCHAR,
    referral_date VARCHAR,
    referral_provider VARCHAR,
    line_of_business VARCHAR,
    network_name VARCHAR,
    account_id VARCHAR,
    provider_id VARCHAR,
    hrk_account_id VARCHAR,
    provider_account_id VARCHAR,
    provider_effective_date VARCHAR,
    provider_tremination_date VARCHAR,
    region_id VARCHAR,
    region_category VARCHAR,
    region_start_date VARCHAR,
    region_end_date VARCHAR,
    created_by VARCHAR,
    created_date VARCHAR,
    updated_by VARCHAR,
    updated_date VARCHAR,
    streetaddress1 VARCHAR,
    streetaddress2 VARCHAR,
    alias VARCHAR,
    alert_desc VARCHAR,
    marital_status VARCHAR,
    marital_status_code VARCHAR,
    w4h_points VARCHAR,
    race_code VARCHAR,
    adt_phone VARCHAR,
    provider_tin VARCHAR,
    subgroupname VARCHAR,
    dob_d VARCHAR,
    labtest_id VARCHAR,
    labtest_date VARCHAR,
    hie_source_name_alias VARCHAR,
    hie_source_name VARCHAR,
    hie_source_code VARCHAR,
    attending_physician_name VARCHAR,
    ordering_physician_name VARCHAR,
    test_type VARCHAR,
    notes VARCHAR,
    labtest_type_code VARCHAR,
    admission_date VARCHAR,
    id VARCHAR,
    id_no VARCHAR
)
WITH (
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/nucleo/analytical/patient_details_test',
    PARTITIONED_BY = ARRAY['provider_tin'],
    CHECKPOINT_INTERVAL = 5
);