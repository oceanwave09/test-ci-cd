BEGIN;

CREATE TABLE IF NOT EXISTS claim_response_add_item_provider (
    id                  SERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    claim_response_id   BIGINT REFERENCES claim_response (id),
    provider_id         UUID NOT NULL,
    provider_type       VARCHAR(50) NOT NULL
);
CREATE INDEX claim_response_add_item_provider_claim_response_id_idx ON claim_response_add_item_provider(claim_response_id);

CREATE TABLE IF NOT EXISTS claim_response_add_item_provider_history (
    id                  SERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    claim_response_id   BIGINT REFERENCES claim_response (id),
    provider_id         UUID NOT NULL,
    provider_type       VARCHAR(50) NOT NULL
);

COMMIT;

END;