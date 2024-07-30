BEGIN;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS adverse_event (
    id                     SERIAL PRIMARY KEY,
    adverse_event_id UUID  NOT NULL UNIQUE default uuid_generate_v4(),
    patient_id             BIGINT NOT NULL,
    encounter_id           BIGINT NOT NULL,
    data                   JSONB NOT NULL,
    fhir_version           TEXT  NOT NULL,
    created_at             TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at             TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by             VARCHAR(255) NOT NULL,
    FOREIGN KEY (patient_id) REFERENCES patient (id),
    FOREIGN KEY (encounter_id) REFERENCES encounter (id)
);

CREATE INDEX adverse_event_patient_id_idx ON adverse_event(patient_id);
CREATE INDEX adverse_event_encounter_id_idx ON adverse_event(encounter_id);

CREATE TABLE IF NOT EXISTS adverse_event_history (
    id                  SERIAL PRIMARY KEY,
    adverse_event_id    UUID NOT NULL,
    data                JSONB NOT NULL,
    fhir_version        TEXT NOT NULL,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by          VARCHAR(255) NOT NULL
);

CREATE INDEX adverse_event_history_adverse_event_id_idx ON adverse_event_history(adverse_event_id);

COMMIT;

END;