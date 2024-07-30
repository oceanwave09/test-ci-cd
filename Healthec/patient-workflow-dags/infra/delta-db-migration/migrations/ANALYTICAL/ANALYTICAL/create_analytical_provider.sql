CREATE TABLE IF NOT EXISTS TENANT_analytical.provider (
    provider_fhir_id VARCHAR,
    internal_id VARCHAR,
    ssn VARCHAR,
    npi VARCHAR,
    active BOOLEAN,   
    first_name VARCHAR,
    last_name VARCHAR,
    middle_initial VARCHAR,
    name_prefix VARCHAR,
    name_suffix VARCHAR,
    phone_work VARCHAR,
    phone_mobile VARCHAR,
    email VARCHAR,
    fax VARCHAR,
    street_line_1 VARCHAR,
    street_line_2 VARCHAR,
    city VARCHAR,
    county VARCHAR,
    state VARCHAR,
    zip VARCHAR,
    country VARCHAR,
    gender VARCHAR,
    birth_date DATE,
    qualification_codes ARRAY(VARCHAR),
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/analytical/provider',
    CHECKPOINT_INTERVAL = 5
);
