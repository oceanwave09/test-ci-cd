BEGIN;

CREATE TABLE IF NOT EXISTS medication_dispense (
    id                           SERIAL PRIMARY KEY,
    medication_dispense_id       UUID  NOT NULL UNIQUE default uuid_generate_v4(),
    encounter_id                 BIGINT REFERENCES encounter (id),
    episode_of_care_id           BIGINT REFERENCES episode_of_care (id),
    patient_id                   BIGINT NOT NULL REFERENCES patient(id),
    data                         JSONB NOT NULL,
    fhir_version                 TEXT  NOT NULL,
    created_at                   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at                   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by                   VARCHAR(255) NOT NULL,
    CONSTRAINT encounter_or_episode_of_care_chk CHECK ((episode_of_care_id IS NULL) <> (encounter_id IS NULL))
);

CREATE TABLE IF NOT EXISTS medication_dispense_medication_request (
    id SERIAL PRIMARY KEY,
    medication_dispense_id BIGINT references medication_dispense(id),
    medication_request_id BIGINT references medication_request(id)
);

COMMIT;

END;