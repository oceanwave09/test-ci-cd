BEGIN;

CREATE TABLE IF NOT EXISTS claim (
    id                  SERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    claim_id            UUID NOT NULL UNIQUE    default uuid_generate_v4(),
    patient_id          UUID NOT NULL,
    enterer_id          UUID DEFAULT NULL,
    insurer_id          UUID DEFAULT NULL,
    provider_id         UUID DEFAULT NULL,
    facility_id         UUID DEFAULT NULL,
    period_start        TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    period_end          TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    data                JSONB NOT NULL,
    fhir_version        TEXT  NOT NULL,
    updated_by          VARCHAR(255) NOT NULL,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX claim_tenant_id_idx ON claim(tenant_id);
CREATE INDEX claim_claim_id_idx ON claim(claim_id);

CREATE TABLE IF NOT EXISTS claim_history (
    id                  SERIAL PRIMARY KEY,
    claim_id            UUID  NOT NULL,
    patient_id          UUID NOT NULL,
    enterer_id          UUID DEFAULT NULL,
    insurer_id          UUID DEFAULT NULL,
    provider_id         UUID DEFAULT NULL,
    facility_id         UUID DEFAULT NULL,
    period_start        TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    period_end          TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    data                JSONB NOT NULL,
    fhir_version        TEXT  NOT NULL,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by          VARCHAR(255) NOT NULL
);
CREATE INDEX claim_history_claim_id_idx ON claim_history(claim_id);

COMMIT;

END;