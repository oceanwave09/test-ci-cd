CREATE TABLE IF NOT EXISTS baco_analytical.patient (
    id VARCHAR,
    birth_date DATE,
    death_date DATE,
    ssn VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    marital_status VARCHAR,
    race VARCHAR,
    ethnicity VARCHAR,
    gender VARCHAR,
    birth_place VARCHAR,
    street VARCHAR,
    city VARCHAR,
    state VARCHAR,
    county VARCHAR,
    zip VARCHAR,
    healthcare_expenses DOUBLE,
    healthcare_coverage DOUBLE,
    facility_id INTEGER
)
WITH (
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/baco/analytical/patient',
    PARTITIONED_BY = ARRAY['facility_id'],
    CHECKPOINT_INTERVAL = 5
);