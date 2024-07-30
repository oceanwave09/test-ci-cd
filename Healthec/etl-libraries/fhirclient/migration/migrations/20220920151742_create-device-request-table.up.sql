BEGIN;

CREATE TABLE IF NOT EXISTS device_request
(
    id                SERIAL PRIMARY KEY,
    device_request_id UUID         NOT NULL UNIQUE default uuid_generate_v4(),
    patient_id        BIGINT       NOT NULL,
    tenant_id         TEXT         NOT NULL,
    data              JSONB        NOT NULL,
    fhir_version      TEXT         NOT NULL,
    created_at        TIMESTAMP WITH TIME ZONE     DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP WITH TIME ZONE     DEFAULT CURRENT_TIMESTAMP,
    updated_by        VARCHAR(255) NOT NULL,
    FOREIGN KEY (patient_id) REFERENCES patient (id)
);

CREATE INDEX device_request_idx ON device_request (device_request_id);

COMMIT;

END;