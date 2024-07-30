BEGIN;

CREATE TABLE IF NOT EXISTS claim_response_add_item_location (
    id                  SERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    claim_response_id   BIGINT REFERENCES claim_response (id),
    location_id         UUID NOT NULL
);
CREATE INDEX claim_response_add_item_location_claim_response_id_idx ON claim_response_add_item_location(claim_response_id);

CREATE TABLE IF NOT EXISTS claim_response_add_item_location_history (
    id                  SERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    claim_response_id   BIGINT REFERENCES claim_response (id),
    location_id         UUID NOT NULL
);

COMMIT;

END;