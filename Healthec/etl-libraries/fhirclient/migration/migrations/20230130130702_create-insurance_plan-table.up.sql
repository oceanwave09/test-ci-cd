BEGIN;

CREATE TABLE IF NOT EXISTS insurance_plan (
    id                  SERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    insurance_plan_id   UUID NOT NULL UNIQUE    default uuid_generate_v4(),
    owned_by            UUID DEFAULT NULL,
    administered_by     UUID DEFAULT NULL,
    period_start        TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    period_end          TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    data                JSONB NOT NULL,
    fhir_version        TEXT  NOT NULL,
    updated_by          VARCHAR(255) NOT NULL,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX insurance_plan_tenant_id_idx ON insurance_plan(tenant_id);
CREATE INDEX insurance_plan_insurance_plan_id_idx ON insurance_plan(insurance_plan_id);

CREATE TABLE IF NOT EXISTS insurance_plan_history (
    id                  SERIAL PRIMARY KEY,
    insurance_plan_id   UUID  NOT NULL,
    owned_by            UUID DEFAULT NULL,
    administered_by     UUID DEFAULT NULL,
    period_start        TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    period_end          TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    data                JSONB NOT NULL,
    fhir_version        TEXT  NOT NULL,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by          VARCHAR(255) NOT NULL
);
CREATE INDEX insurance_plan_history_insurance_plan_id_idx ON insurance_plan_history(insurance_plan_id);

COMMIT;

END;