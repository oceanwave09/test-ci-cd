BEGIN;

CREATE TABLE IF NOT EXISTS coverage_payment_by_party (
    id            SERIAL PRIMARY KEY,
    tenant_id     TEXT NOT NULL,
    coverage_id   BIGINT REFERENCES coverage (id),
    party_id      UUID NOT NULL,
    party_type    VARCHAR(50) NOT NULL
);
CREATE INDEX coverage_payment_by_party_coverage_id_idx ON coverage_payment_by_party(coverage_id);

CREATE TABLE IF NOT EXISTS coverage_payment_by_party_history (
    id            SERIAL PRIMARY KEY,
    tenant_id     TEXT NOT NULL,
    coverage_id   BIGINT REFERENCES coverage (id),
    party_id      UUID NOT NULL,
    party_type    VARCHAR(50) NOT NULL
);

COMMIT;

END;