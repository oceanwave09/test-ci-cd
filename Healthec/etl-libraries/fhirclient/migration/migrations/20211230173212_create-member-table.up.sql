BEGIN;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS patient (
    id           SERIAL PRIMARY KEY,
    tenant_id    TEXT  NOT NULL,
    patient_id   UUID  NOT NULL UNIQUE default uuid_generate_v4(),
    data         JSONB NOT NULL,
    fhir_version TEXT  NOT NULL,
    created_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by   VARCHAR(255) NOT NULL
);
CREATE INDEX patient_tenant_id_idx ON patient(tenant_id);

CREATE TABLE IF NOT EXISTS patient_history (
    id           SERIAL PRIMARY KEY,
    patient_id   UUID  NOT NULL,
    data         JSONB NOT NULL,
    fhir_version TEXT  NOT NULL,
    created_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by   VARCHAR(255) NOT NULL
);

-- link_type has max length 11 because the value with a max length is "replaced-by" (11)
CREATE TABLE IF NOT EXISTS patient_link (
    id                      SERIAL PRIMARY KEY,
    patient_id              BIGINT NOT NULL,
    linked_patient_id       BIGINT NOT NULL,
    linked_patient_uuid     UUID  NOT NULL,
    link_type               VARCHAR(11) NOT NULL,
    FOREIGN KEY (patient_id) REFERENCES patient (id),
    FOREIGN KEY (linked_patient_id) REFERENCES patient (id)
);
CREATE INDEX patient_link_patient_id_link_type_idx ON patient_link(patient_id, link_type);

CREATE TABLE IF NOT EXISTS allergy_intolerance (
    id                     SERIAL PRIMARY KEY,
    allergy_intolerance_id UUID  NOT NULL UNIQUE default uuid_generate_v4(),
    patient_id             BIGINT NOT NULL,
    data                   JSONB NOT NULL,
    fhir_version           TEXT  NOT NULL,
    created_at             TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at             TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by             VARCHAR(255) NOT NULL,
    FOREIGN KEY (patient_id) REFERENCES patient (id)
);
CREATE INDEX allergy_intolerance_patient_id_idx ON allergy_intolerance(patient_id);

CREATE TABLE IF NOT EXISTS allergy_intolerance_history (
    id                      SERIAL PRIMARY KEY,
    allergy_intolerance_id  UUID  NOT NULL,
    data                    JSONB NOT NULL,
    fhir_version            TEXT  NOT NULL,
    created_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by              VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS care_plan (
   id               SERIAL PRIMARY KEY,
   patient_id       INT NOT NULL,
   care_plan_id     UUID NOT NULL UNIQUE default uuid_generate_v4(),
   data             JSONB NOT NULL,
   fhir_version     TEXT NOT NULL,
   created_at       TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
   updated_at       TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
   updated_by       VARCHAR(255) NOT NULL
);
CREATE INDEX care_plan_patient_id_idx ON care_plan(patient_id);

CREATE TABLE IF NOT EXISTS care_plan_history (
   id              SERIAL PRIMARY KEY,
   care_plan_id    UUID NOT NULL,
   data            JSONB NOT NULL,
   fhir_version    TEXT  NOT NULL,
   created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
   updated_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
   updated_by      VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS condition (
    id           SERIAL PRIMARY KEY,
    condition_id UUID  NOT NULL UNIQUE default uuid_generate_v4(),
    patient_id   BIGINT NOT NULL,
    data         JSONB NOT NULL,
    fhir_version TEXT  NOT NULL,
    created_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by   VARCHAR(255) NOT NULL,
    FOREIGN KEY (patient_id) REFERENCES patient (id)
);
CREATE INDEX condition_patient_id_idx ON condition(patient_id);

CREATE TABLE IF NOT EXISTS condition_history (
    id              SERIAL PRIMARY KEY,
    condition_id    UUID  NOT NULL,
    data            JSONB NOT NULL,
    fhir_version    TEXT  NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by      VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS device (
    id           SERIAL PRIMARY KEY,
    tenant_id    TEXT  NOT NULL,
    device_id    UUID  NOT NULL UNIQUE default uuid_generate_v4(),
    data         JSONB NOT NULL,
    fhir_version TEXT  NOT NULL,
    created_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by   VARCHAR(255) NOT NULL
);
CREATE INDEX device_tenant_id_idx ON device(tenant_id);

CREATE TABLE IF NOT EXISTS device_history (
    id              SERIAL PRIMARY KEY,
    device_id       UUID  NOT NULL,
    data            JSONB NOT NULL,
    fhir_version    TEXT  NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by      VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS encounter (
    id           SERIAL PRIMARY KEY,
    encounter_id UUID  NOT NULL UNIQUE default uuid_generate_v4(),
    patient_id   BIGINT NOT NULL,
    data         JSONB NOT NULL,
    fhir_version TEXT  NOT NULL,
    created_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by   VARCHAR(255) NOT NULL,
    FOREIGN KEY (patient_id) REFERENCES patient (id)
);
CREATE INDEX encounter_patient_id_idx ON encounter(patient_id);

CREATE TABLE IF NOT EXISTS encounter_history (
    id                   SERIAL PRIMARY KEY,
    encounter_id         UUID  NOT NULL,
    data                 JSONB NOT NULL,
    fhir_version         TEXT  NOT NULL,
    created_at           TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by           VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS immunization (
    id              SERIAL PRIMARY KEY,
    immunization_id UUID  NOT NULL UNIQUE default uuid_generate_v4(),
    patient_id      BIGINT NOT NULL,
    data            JSONB NOT NULL,
    fhir_version    TEXT  NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by      VARCHAR(255) NOT NULL,
    FOREIGN KEY (patient_id) REFERENCES patient (id)
);
CREATE INDEX immunization_patient_id_idx ON immunization(patient_id);

CREATE TABLE IF NOT EXISTS immunization_history (
    id                 SERIAL PRIMARY KEY,
    immunization_id    UUID  NOT NULL,
    data               JSONB NOT NULL,
    fhir_version       TEXT  NOT NULL,
    created_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by         VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS intervention (
   id               SERIAL PRIMARY KEY,
   member_id        TEXT NOT NULL UNIQUE,
   intervention_id  TEXT NOT NULL UNIQUE,
   created_at       TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
   updated_at       TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
   updated_by       VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS lab_result (
   id               SERIAL PRIMARY KEY,
   member_id        TEXT NOT NULL UNIQUE,
   lab_result_id    TEXT NOT NULL UNIQUE,
   created_at       TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
   updated_at       TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
   updated_by       VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS medication (
    id            SERIAL PRIMARY KEY,
    tenant_id     TEXT  NOT NULL,
    medication_id UUID  NOT NULL UNIQUE default uuid_generate_v4(),
    data          JSONB NOT NULL,
    fhir_version  TEXT  NOT NULL,
    created_at    TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by    VARCHAR(255) NOT NULL
);
CREATE INDEX medication_tenant_id_idx ON medication(tenant_id);

CREATE TABLE IF NOT EXISTS medication_history (
    id               SERIAL PRIMARY KEY,
    medication_id    UUID  NOT NULL,
    data             JSONB NOT NULL,
    fhir_version     TEXT  NOT NULL,
    created_at       TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by       VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS procedure (
    id           SERIAL PRIMARY KEY,
    procedure_id UUID  NOT NULL UNIQUE default uuid_generate_v4(),
    patient_id   BIGINT NOT NULL,
    data         JSONB NOT NULL,
    fhir_version TEXT  NOT NULL,
    created_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by   VARCHAR(255) NOT NULL,
    FOREIGN KEY (patient_id) REFERENCES patient (id)
);
CREATE INDEX procedure_patient_id_idx ON procedure(patient_id);

CREATE TABLE IF NOT EXISTS procedure_history (
    id              SERIAL PRIMARY KEY,
    procedure_id    UUID  NOT NULL,
    data            JSONB NOT NULL,
    fhir_version    TEXT  NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by      VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS observation (
    id             SERIAL PRIMARY KEY,
    observation_id UUID  NOT NULL UNIQUE default uuid_generate_v4(),
    patient_id     INT   NOT NULL,
    encounter_id   INT   DEFAULT NULL,
    data           JSONB NOT NULL,
    fhir_version   TEXT  NOT NULL,
    created_at     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by     VARCHAR(255) NOT NULL,
    FOREIGN KEY (patient_id) REFERENCES patient (id),
    FOREIGN KEY (encounter_id) REFERENCES encounter (id)
);
CREATE INDEX observation_patient_id_idx ON observation(patient_id);
CREATE INDEX observation_encounter_id_idx ON observation(encounter_id);

CREATE TABLE IF NOT EXISTS observation_history (
    id               SERIAL PRIMARY KEY,
    observation_id   UUID  NOT NULL,
    data             JSONB NOT NULL,
    fhir_version     TEXT  NOT NULL,
    created_at       TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by       VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS medication_administration (
    id                           SERIAL PRIMARY KEY,
    medication_administration_id UUID  NOT NULL UNIQUE default uuid_generate_v4(),
    patient_id                   BIGINT NOT NULL,
    data                         JSONB NOT NULL,
    fhir_version                 TEXT  NOT NULL,
    created_at                   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at                   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by                   VARCHAR(255) NOT NULL,
    FOREIGN KEY (patient_id) REFERENCES patient (id)
);
CREATE INDEX medication_administration_patient_id_idx ON medication_administration(patient_id);

CREATE TABLE IF NOT EXISTS medication_administration_history (
    id                              SERIAL PRIMARY KEY,
    medication_administration_id    UUID  NOT NULL,
    data                            JSONB NOT NULL,
    fhir_version                    TEXT  NOT NULL,
    created_at                      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at                      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by                      VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS medication_statement (
    id                      SERIAL PRIMARY KEY,
    medication_statement_id UUID  NOT NULL UNIQUE default uuid_generate_v4(),
    patient_id              BIGINT NOT NULL,
    data                    JSONB NOT NULL,
    fhir_version            TEXT  NOT NULL,
    created_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by              VARCHAR(255) NOT NULL,
    FOREIGN KEY (patient_id) REFERENCES patient (id)
);
CREATE INDEX medication_statement_patient_id_idx ON medication_statement(patient_id);

CREATE TABLE IF NOT EXISTS medication_statement_history (
    id                              SERIAL PRIMARY KEY,
    medication_statement_id         UUID  NOT NULL,
    data                            JSONB NOT NULL,
    fhir_version                    TEXT  NOT NULL,
    created_at                      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at                      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by                      VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS episode_of_care (
    id                 SERIAL PRIMARY KEY,
    episode_of_care_id UUID  NOT NULL UNIQUE default uuid_generate_v4(),
    patient_id         BIGINT NOT NULL,
    data               JSONB NOT NULL,
    fhir_version       TEXT  NOT NULL,
    created_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by         VARCHAR(255) NOT NULL,
    FOREIGN KEY (patient_id) REFERENCES patient (id)
);
CREATE INDEX episode_of_care_patient_id_idx ON episode_of_care(patient_id);

CREATE TABLE IF NOT EXISTS episode_of_care_history (
    id                   SERIAL PRIMARY KEY,
    episode_of_care_id   UUID  NOT NULL,
    data                 JSONB NOT NULL,
    fhir_version         TEXT  NOT NULL,
    created_at           TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by           VARCHAR(255) NOT NULL
);

COMMIT;

END;