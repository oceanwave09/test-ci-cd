BEGIN;

CREATE TABLE IF NOT EXISTS explanation_of_benefit_condition (
    id                          SERIAL PRIMARY KEY,
    tenant_id                   TEXT NOT NULL,
    explanation_of_benefit_id   BIGINT REFERENCES explanation_of_benefit (id),
    condition_id                UUID NOT NULL
);
CREATE INDEX explanation_of_benefit_condition_explanation_of_benefit_id_idx
    ON explanation_of_benefit_condition(explanation_of_benefit_id);

CREATE TABLE IF NOT EXISTS explanation_of_benefit_condition_history (
    id                          SERIAL PRIMARY KEY,
    tenant_id                   TEXT NOT NULL,
    explanation_of_benefit_id   BIGINT REFERENCES explanation_of_benefit (id),
    condition_id                UUID NOT NULL
);

COMMIT;

END;