BEGIN;

ALTER TABLE IF EXISTS organization_identifier
    DROP COLUMN IF EXISTS assigner_id;

COMMIT;