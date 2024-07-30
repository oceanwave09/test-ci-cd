BEGIN;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS organization (
    id                      SERIAL PRIMARY KEY,
    tenant_id               TEXT  NOT NULL,
    organization_id         UUID  NOT NULL UNIQUE    default uuid_generate_v4(),
    data                    JSONB NOT NULL,
    fhir_version            TEXT  NOT NULL,
    updated_by              VARCHAR(255) NOT NULL,
    created_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX organization_tenant_id_idx ON organization(tenant_id);

CREATE TABLE IF NOT EXISTS organization_history (
    id                  SERIAL PRIMARY KEY,
    organization_id     UUID  NOT NULL,
    data                JSONB NOT NULL,
    fhir_version        TEXT  NOT NULL,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by          VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS organization_type (
    id                  SERIAL PRIMARY KEY,
    organization_id     BIGINT  NOT NULL,
    code                TEXT    NOT NULL,
    display             TEXT    NOT NULL,
    system              TEXT    NOT NULL,
    FOREIGN KEY (organization_id) REFERENCES organization (id)
);
CREATE INDEX organization_type_code_idx ON organization_type(code);
CREATE INDEX organization_id_type_code_idx ON organization_type(organization_id);

CREATE TABLE IF NOT EXISTS organization_type_history (
    id                  SERIAL PRIMARY KEY,
    organization_id     INT     NOT NULL,
    code                TEXT    NOT NULL,
    display             TEXT    NOT NULL,
    system              TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS location (
    id              serial PRIMARY KEY,
    location_id     UUID  NOT NULL UNIQUE    default uuid_generate_v4(),
    organization_id BIGINT NOT NULL,
    data            JSONB NOT NULL,
    fhir_version    TEXT  NOT NULL,
    updated_by      VARCHAR(255) NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (organization_id) REFERENCES ORGANIZATION (id)
);
CREATE INDEX location_organization_id_idx ON location(organization_id);

CREATE TABLE IF NOT EXISTS location_history (
    id                  SERIAL PRIMARY KEY,
    location_id         UUID  NOT NULL,
    data                JSONB NOT NULL,
    fhir_version        TEXT  NOT NULL,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by          VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS organization_affiliation (
    id                          serial PRIMARY KEY,
    organization_affiliation_id UUID  NOT NULL UNIQUE    default uuid_generate_v4(),
    organization_id             BIGINT NOT NULL,
    data                        JSONB NOT NULL,
    fhir_version                TEXT  NOT NULL,
    updated_by                  VARCHAR(255) NOT NULL,
    created_at                  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at                  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (organization_id) REFERENCES ORGANIZATION (id)
);
CREATE INDEX organization_affiliation_organization_id_idx ON organization_affiliation(organization_id);

CREATE TABLE IF NOT EXISTS organization_affiliation_history (
    id                              SERIAL PRIMARY KEY,
    organization_affiliation_id     UUID  NOT NULL,
    data                            JSONB NOT NULL,
    fhir_version                    TEXT  NOT NULL,
    created_at                      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at                      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by                      VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS organization_measure (
    id                          serial PRIMARY KEY,
    organization_measure_id     UUID NOT NULL UNIQUE    default uuid_generate_v4(),
    organization_id             BIGINT NOT NULL,
    measure_id                  TEXT  NOT NULL,
    measure_version             TEXT  NOT NULL,
    updated_by                  VARCHAR(255) NOT NULL,
    created_at                  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at                  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (organization_id) REFERENCES organization (id)
);
CREATE INDEX organization_measure_organization_id_idx ON organization_affiliation(organization_id);

CREATE TABLE IF NOT EXISTS organization_measure_history (
    id                          SERIAL PRIMARY KEY,
    organization_measure_id     UUID  NOT NULL,
    measure_id                  TEXT  NOT NULL,
    measure_version             TEXT  NOT NULL,
    created_at                  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at                  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by                  VARCHAR(255) NOT NULL
);

COMMIT;

END;
