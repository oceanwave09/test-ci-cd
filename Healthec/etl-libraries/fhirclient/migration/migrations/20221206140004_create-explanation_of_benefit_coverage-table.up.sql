BEGIN;

CREATE TABLE IF NOT EXISTS explanation_of_benefit_coverage (
    id                          SERIAL PRIMARY KEY,
    tenant_id                   TEXT NOT NULL,
    explanation_of_benefit_id   BIGINT REFERENCES explanation_of_benefit (id),
    coverage_id                 BIGINT REFERENCES coverage (id)
);
CREATE INDEX explanation_of_benefit_coverage_explanation_of_benefit_id_idx
    ON explanation_of_benefit_coverage(explanation_of_benefit_id);
CREATE INDEX explanation_of_benefit_coverage_coverage_id_idx
    ON explanation_of_benefit_coverage(coverage_id);

CREATE TABLE IF NOT EXISTS explanation_of_benefit_coverage_history (
    id                          SERIAL PRIMARY KEY,
    tenant_id                   TEXT NOT NULL,
    explanation_of_benefit_id   BIGINT REFERENCES explanation_of_benefit (id),
    coverage_id                 BIGINT REFERENCES coverage (id)
);

COMMIT;

END;