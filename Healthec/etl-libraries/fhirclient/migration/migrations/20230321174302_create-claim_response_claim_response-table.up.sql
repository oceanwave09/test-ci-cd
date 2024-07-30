BEGIN;

CREATE TABLE IF NOT EXISTS claim_response_claim_response (
    id                  SERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    claim_response_id   BIGINT REFERENCES claim_response (id),
    related_claim_response_id BIGINT REFERENCES claim_response (id)
);
CREATE INDEX claim_response_claim_response_claim_response_id_idx ON claim_response_claim_response(claim_response_id);
CREATE INDEX claim_response_claim_response_related_claim_response_id_idx ON claim_response_claim_response(related_claim_response_id);

CREATE TABLE IF NOT EXISTS claim_response_claim_response_history (
    id                  SERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    claim_response_id   BIGINT REFERENCES claim_response (id),
    related_claim_response_id BIGINT REFERENCES claim_response (id)
);

COMMIT;

END;