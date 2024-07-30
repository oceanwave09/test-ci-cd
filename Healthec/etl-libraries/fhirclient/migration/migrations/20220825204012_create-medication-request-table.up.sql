BEGIN;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS medication_request (
    id                            SERIAL PRIMARY KEY,
    medication_request_id         UUID NOT NULL UNIQUE default uuid_generate_v4(),
    patient_id                    BIGINT NOT NULL,
    encounter_id                  BIGINT DEFAULT NULL,
    data                          JSONB NOT NULL,
    fhir_version TEXT             NOT NULL,
    created_at                    TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at                    TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by                    VARCHAR(255) NOT NULL,
    FOREIGN KEY (patient_id)      REFERENCES patient (id),
    FOREIGN KEY (encounter_id)    REFERENCES encounter (id)
);
CREATE INDEX medication_request_patient_id_idx ON medication_request(patient_id);

CREATE TABLE IF NOT EXISTS medication_request_history (
    id                       SERIAL PRIMARY KEY,
    medication_request_id    UUID NOT NULL,
    data                     JSONB NOT NULL,
    fhir_version             TEXT NOT NULL,
    created_at               TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at               TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by               VARCHAR(255) NOT NULL
);

COMMIT;

END;