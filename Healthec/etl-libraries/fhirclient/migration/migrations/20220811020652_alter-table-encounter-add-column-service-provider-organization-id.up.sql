BEGIN;

ALTER TABLE IF EXISTS encounter
    ADD COLUMN IF NOT EXISTS service_provider_organization_id TEXT DEFAULT NULL;

COMMIT;

END;