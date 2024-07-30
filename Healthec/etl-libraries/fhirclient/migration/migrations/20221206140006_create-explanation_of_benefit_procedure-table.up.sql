BEGIN;

CREATE TABLE IF NOT EXISTS explanation_of_benefit_procedure (
    id                          SERIAL PRIMARY KEY,
    tenant_id                   TEXT NOT NULL,
    explanation_of_benefit_id   BIGINT REFERENCES explanation_of_benefit (id),
    procedure_id                UUID NOT NULL
);
CREATE INDEX explanation_of_benefit_procedure_explanation_of_benefit_id_idx
    ON explanation_of_benefit_procedure(explanation_of_benefit_id);

CREATE TABLE IF NOT EXISTS explanation_of_benefit_procedure_history (
    id                          SERIAL PRIMARY KEY,
    tenant_id                   TEXT NOT NULL,
    explanation_of_benefit_id   BIGINT REFERENCES explanation_of_benefit (id),
    procedure_id                UUID NOT NULL
);

COMMIT;

END;