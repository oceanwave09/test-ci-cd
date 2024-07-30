BEGIN;

CREATE TABLE IF NOT EXISTS claim_response_insurance_coverage (
    id                  SERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    claim_response_id   BIGINT REFERENCES claim_response (id),
    coverage_id         BIGINT REFERENCES coverage (id)
);
CREATE INDEX claim_response_insurance_coverage_claim_response_id_idx ON claim_response_insurance_coverage(claim_response_id);
CREATE INDEX claim_response_insurance_coverage_coverage_idx ON claim_response_insurance_coverage(coverage_id);

CREATE TABLE IF NOT EXISTS claim_response_insurance_coverage_history (
    id                  SERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    claim_response_id   BIGINT REFERENCES claim_response (id),
    coverage_id         BIGINT REFERENCES coverage (id)
);

COMMIT;

END;