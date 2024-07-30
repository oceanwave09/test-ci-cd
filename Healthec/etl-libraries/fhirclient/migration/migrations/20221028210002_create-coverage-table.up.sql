BEGIN;

CREATE TABLE IF NOT EXISTS coverage (
    id                  SERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    coverage_id         UUID NOT NULL UNIQUE    default uuid_generate_v4(),
    beneficiary_id      UUID NOT NULL,
    payor_id            UUID NOT NULL,
    period_start        TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    period_end          TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    data                JSONB NOT NULL,
    fhir_version        TEXT  NOT NULL,
    updated_by          VARCHAR(255) NOT NULL,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX coverage_tenant_id_idx ON coverage(tenant_id);

CREATE TABLE IF NOT EXISTS coverage_history (
    id                  SERIAL PRIMARY KEY,
    coverage_id         UUID  NOT NULL,
    beneficiary_id      UUID NOT NULL,
    payor_id            UUID NOT NULL,
    period_start        TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    period_end          TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    data                JSONB NOT NULL,
    fhir_version        TEXT  NOT NULL,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by          VARCHAR(255) NOT NULL
);

COMMIT;

END;