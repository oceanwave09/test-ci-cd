BEGIN;

CREATE TABLE IF NOT EXISTS explanation_of_benefit (
    id                  SERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    explanation_of_benefit_id  UUID NOT NULL UNIQUE    default uuid_generate_v4(),
    claim_id            BIGINT REFERENCES claim (id),
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
CREATE INDEX explanation_of_benefit_tenant_id_idx ON explanation_of_benefit(tenant_id);
CREATE INDEX explanation_of_benefit_explanation_of_benefit_id_idx ON explanation_of_benefit(explanation_of_benefit_id);

CREATE TABLE IF NOT EXISTS explanation_of_benefit_history (
    id                  SERIAL PRIMARY KEY,
    explanation_of_benefit_id  UUID  NOT NULL,
    claim_id            BIGINT REFERENCES claim (id),
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
CREATE INDEX explanation_of_benefit_history_explanation_of_benefit_id_idx ON explanation_of_benefit_history(explanation_of_benefit_id);

COMMIT;

END;