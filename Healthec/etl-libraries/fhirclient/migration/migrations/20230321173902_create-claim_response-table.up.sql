BEGIN;

CREATE TABLE IF NOT EXISTS claim_response (
    id                  SERIAL PRIMARY KEY,
    tenant_id           TEXT NOT NULL,
    claim_response_id   UUID NOT NULL UNIQUE    default uuid_generate_v4(),
    patient_id          UUID NOT NULL,
    insurer_id          UUID NOT NULL,
    requestor_id        UUID DEFAULT NULL,
    requestor_type      VARCHAR(50) NOT NULL,
    request_id          UUID DEFAULT NULL,
    created             TIMESTAMP WITH TIME ZONE NOT NULL,
    data                JSONB NOT NULL,
    fhir_version        TEXT  NOT NULL,
    updated_by          VARCHAR(255) NOT NULL,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX claim_response_tenant_id_idx ON claim_response(tenant_id);
CREATE INDEX claim_response_claim_response_id_idx ON claim_response(claim_response_id);

CREATE TABLE IF NOT EXISTS claim_response_history (
    id                  SERIAL PRIMARY KEY,
    claim_response_id   UUID NOT NULL,
    patient_id          UUID NOT NULL,
    insurer_id          UUID NOT NULL,
    requestor_id        UUID DEFAULT NULL,
    requestor_type      VARCHAR(50) NOT NULL,
    request_id          UUID DEFAULT NULL,
    created             TIMESTAMP WITH TIME ZONE NOT NULL,
    data                JSONB NOT NULL,
    fhir_version        TEXT  NOT NULL,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_by          VARCHAR(255) NOT NULL
);
CREATE INDEX claim_response_history_claim_response_id_idx ON claim_response_history(claim_response_id);

COMMIT;

END;