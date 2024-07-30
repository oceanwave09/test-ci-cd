BEGIN;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS patient_identifier (
    id              SERIAL PRIMARY KEY,
    patient_id      BIGINT NOT NULL,
    use             TEXT,
    system          TEXT NOT NULL,
    code_system     TEXT,
    value           TEXT NOT NULL,
    code            TEXT,
    display         TEXT,
    period_start    TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    period_end      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by      VARCHAR(255) NOT NULL,
    FOREIGN KEY (patient_id) REFERENCES patient (id)
);


CREATE INDEX patient_identifier_code_value_system_idx ON patient_identifier(code, value, system);
CREATE INDEX patient_identifier_value_system_idx ON patient_identifier(value, system);

COMMIT;

END;