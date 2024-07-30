BEGIN;

CREATE TABLE IF NOT EXISTS claim_coverage (
    id                  SERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    claim_id            BIGINT REFERENCES claim (id),
    coverage_id         BIGINT REFERENCES coverage (id)
);
CREATE INDEX claim_coverage_claim_id_idx ON claim_coverage(claim_id);
CREATE INDEX claim_coverage_coverage_id_idx ON claim_coverage(coverage_id);

CREATE TABLE IF NOT EXISTS claim_coverage_history (
    id                  SERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    claim_id            BIGINT REFERENCES claim (id),
    coverage_id         BIGINT REFERENCES coverage (id)
);

COMMIT;

END;