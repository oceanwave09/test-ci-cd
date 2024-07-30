BEGIN;

CREATE TABLE IF NOT EXISTS insurance_plan_coverage_area (
    id                  SERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    insurance_plan_id   BIGINT REFERENCES insurance_plan (id),
    location_id         UUID NOT NULL
);
CREATE INDEX insurance_plan_coverage_area_insurance_plan_id_idx
    ON insurance_plan_coverage_area(insurance_plan_id);

CREATE TABLE IF NOT EXISTS insurance_plan_coverage_area_history (
    id                  SERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    insurance_plan_id   BIGINT REFERENCES insurance_plan (id),
    location_id         UUID NOT NULL
);

COMMIT;

END;