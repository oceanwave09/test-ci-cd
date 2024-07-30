CREATE TABLE IF NOT EXISTS TENANT_analytical.practice (
    practice_fhir_id VARCHAR,
    internal_id VARCHAR,
    ssn VARCHAR,
    tin VARCHAR,
    group_npi VARCHAR,
    active BOOLEAN,
    type VARCHAR,
    name VARCHAR,
    alias VARCHAR,
    phone_work VARCHAR,
    email VARCHAR,
    fax VARCHAR,
    street_line_1 VARCHAR,
    street_line_2 VARCHAR,
    county VARCHAR,
    state VARCHAR,
    city VARCHAR,
    zip VARCHAR,
    country VARCHAR,
    parent_practice_fhir_id VARCHAR,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/analytical/practice',
    CHECKPOINT_INTERVAL = 5
);
