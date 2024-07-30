CREATE TABLE IF NOT EXISTS cynchealth_analytical.practice (
    fhir_id VARCHAR,
    parent_fhir_id VARCHAR,
    name VARCHAR,
    dba_name VARCHAR,
    external_id VARCHAR,
    ssn VARCHAR,
    tin VARCHAR,
    group_npi VARCHAR,
    nabp_id VARCHAR,
    type_code VARCHAR,
    type_system VARCHAR,
    type_desc VARCHAR,
    address_line_1 VARCHAR,
    address_line_2 VARCHAR,
    county VARCHAR,
    state VARCHAR,
    city VARCHAR,
    zip VARCHAR,
    country VARCHAR,
    phone_work VARCHAR,
    email VARCHAR,
    fax VARCHAR,
    contact_first_name VARCHAR,
    contact_middle_name VARCHAR,
    contact_last_name VARCHAR,
    contact_email VARCHAR,
    contact_phone_work VARCHAR,
    active BOOLEAN,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/cynchealth/analytical/practice',
    CHECKPOINT_INTERVAL = 5
);
