BEGIN;

CREATE TABLE IF NOT EXISTS diagnostic_report (
    id                   SERIAL PRIMARY KEY,
    diagnostic_report_id UUID         NOT NULL UNIQUE default uuid_generate_v4(),
    patient_id           BIGINT       NOT NULL,
    encounter_id         INT          DEFAULT NULL,
    data                 JSONB        NOT NULL,
    fhir_version         TEXT         NOT NULL,
    created_at           TIMESTAMP WITH TIME ZONE     DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP WITH TIME ZONE     DEFAULT CURRENT_TIMESTAMP,
    updated_by           VARCHAR(255) NOT NULL,
    FOREIGN KEY (patient_id) REFERENCES patient (id),
    FOREIGN KEY (encounter_id) REFERENCES encounter (id)
);
CREATE INDEX diagnostic_report_patient_id_idx ON diagnostic_report (patient_id);
CREATE INDEX diagnostic_report_encounter_id_idx ON diagnostic_report (encounter_id);

CREATE TABLE IF NOT EXISTS diagnostic_report_history (
    id                   SERIAL PRIMARY KEY,
    diagnostic_report_id UUID         NOT NULL,
    data                 JSONB        NOT NULL,
    fhir_version         TEXT         NOT NULL,
    created_at           TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by           VARCHAR(255) NOT NULL
);

COMMIT;

END;