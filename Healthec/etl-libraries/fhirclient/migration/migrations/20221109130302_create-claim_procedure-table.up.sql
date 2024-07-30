BEGIN;

CREATE TABLE IF NOT EXISTS claim_procedure (
    id                  SERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    claim_id            BIGINT REFERENCES claim (id),
    procedure_id        UUID NOT NULL
);
CREATE INDEX claim_procedure_claim_id_idx ON claim_procedure(claim_id);

CREATE TABLE IF NOT EXISTS claim_procedure_history (
    id                  SERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    claim_id            BIGINT REFERENCES claim (id),
    procedure_id        UUID NOT NULL
);

COMMIT;

END;