CREATE TABLE IF NOT EXISTS TENANT_analytical.facility (
    facility_fhir_id VARCHAR,
    internal_id VARCHAR,
    group_npi VARCHAR,
    status VARCHAR,
    name VARCHAR,
    alias VARCHAR,
    description VARCHAR,
    mode VARCHAR,
    type_code VARCHAR,
    type_desc VARCHAR,
    phone_work VARCHAR,
    fax VARCHAR,
    email VARCHAR,
    street_line_1 VARCHAR,
    street_line_2 VARCHAR,
    county VARCHAR,
    state VARCHAR,
    city VARCHAR,
    zip VARCHAR,
    practice_fhir_id VARCHAR,
    parent_facility_fhir_id VARCHAR,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://DATA_S3_BUCKET/delta-tables/TENANT/analytical/facility',
    CHECKPOINT_INTERVAL = 5
);
