CREATE TABLE IF NOT EXISTS nucleo_analytical.encounter (
    encounter_fhir_id VARCHAR,
    external_id VARCHAR,
    status VARCHAR,
    class_system VARCHAR,
    class_code VARCHAR,
    class_desc VARCHAR,
    type_system VARCHAR,
    type_code VARCHAR,
    type_desc VARCHAR,
    patient_fhir_id VARCHAR,
    provider_fhir_id VARCHAR,
    start_date TIMESTAMP(3) WITH TIME ZONE,
    end_date TIMESTAMP(3) WITH TIME ZONE,
    reason_system VARCHAR,
    reason_code VARCHAR,
    reason_desc VARCHAR,
    origin_fhir_id VARCHAR,
    destination_fhir_id VARCHAR,
    admit_source_system VARCHAR,
    admit_source_code VARCHAR,
    admit_source_desc VARCHAR,
    discharge_disposition_system VARCHAR,
    discharge_disposition_code VARCHAR,
    discharge_disposition_desc VARCHAR,
    facility_fhir_id VARCHAR,
    updated_user VARCHAR,
    updated_ts TIMESTAMP(3) WITH TIME ZONE
)
WITH (
    LOCATION = 's3a://phm-development-datapipeline-bucket/delta-tables/nucleo/analytical/encounter',
    CHECKPOINT_INTERVAL = 5
);