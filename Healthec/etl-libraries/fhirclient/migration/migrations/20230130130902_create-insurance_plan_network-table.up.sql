BEGIN;

CREATE TABLE IF NOT EXISTS insurance_plan_network (
    id                  SERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    insurance_plan_id   BIGINT REFERENCES insurance_plan (id),
    organization_id     UUID NOT NULL
);
CREATE INDEX insurance_plan_network_insurance_plan_id_idx
    ON insurance_plan_network(insurance_plan_id);

CREATE TABLE IF NOT EXISTS insurance_plan_network_history (
    id                  SERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    insurance_plan_id   BIGINT REFERENCES insurance_plan (id),
    organization_id     UUID NOT NULL
);

COMMIT;

END;