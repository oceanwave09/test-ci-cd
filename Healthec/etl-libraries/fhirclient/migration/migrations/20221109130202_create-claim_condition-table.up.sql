BEGIN;

CREATE TABLE IF NOT EXISTS claim_condition (
    id                  SERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    claim_id            BIGINT REFERENCES claim (id),
    condition_id        UUID NOT NULL
);
CREATE INDEX claim_condition_claim_id_idx ON claim_condition(claim_id);

CREATE TABLE IF NOT EXISTS claim_condition_history (
    id                  SERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    claim_id            BIGINT REFERENCES claim (id),
    condition_id        UUID NOT NULL
);

COMMIT;

END;