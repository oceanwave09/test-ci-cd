BEGIN;

ALTER TABLE IF EXISTS encounter
    DROP COLUMN IF EXISTS service_provider_organization_id;

COMMIT;
