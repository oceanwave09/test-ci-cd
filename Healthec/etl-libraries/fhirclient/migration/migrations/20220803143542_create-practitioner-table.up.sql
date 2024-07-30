CREATE TABLE IF NOT EXISTS practitioner (
    id              SERIAL PRIMARY KEY,
    tenant_id       TEXT  NOT NULL,
    practitioner_id UUID  NOT NULL UNIQUE    default uuid_generate_v4(),
    data            JSONB NOT NULL,
    fhir_version    TEXT  NOT NULL,
    updated_by      VARCHAR(255) NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX practitioner_tenant_id_idx ON practitioner(tenant_id);

CREATE TABLE IF NOT EXISTS practitioner_history (
    id                  SERIAL PRIMARY KEY,
    practitioner_id     UUID  NOT NULL,
    data                JSONB NOT NULL,
    fhir_version        TEXT  NOT NULL,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by          VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS practitioner_role (
    id                      SERIAL PRIMARY KEY,
    tenant_id               TEXT NOT NULL,
    practitioner_role_id    UUID NOT NULL UNIQUE default uuid_generate_v4(),
    practitioner_id         BIGINT NOT NULL,
    organization_id         UUID NOT NULL,
    data                    JSONB NOT NULL,
    fhir_version            TEXT NOT NULL,
    updated_by              VARCHAR(255) NOT NULL,
    created_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (practitioner_id) REFERENCES practitioner (id)
);
CREATE INDEX practitioner_role_organization_id_idx ON practitioner_role(organization_id);
CREATE INDEX practitioner_role_practitioner_id_idx ON practitioner_role(practitioner_id);
CREATE INDEX practitioner_role_tenant_id_idx ON practitioner_role(tenant_id);

CREATE TABLE IF NOT EXISTS practitioner_role_history (
    id                      SERIAL PRIMARY KEY,
    practitioner_role_id    UUID  NOT NULL,
    data                    JSONB NOT NULL,
    fhir_version            TEXT  NOT NULL,
    created_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by              VARCHAR(255) NOT NULL
);
