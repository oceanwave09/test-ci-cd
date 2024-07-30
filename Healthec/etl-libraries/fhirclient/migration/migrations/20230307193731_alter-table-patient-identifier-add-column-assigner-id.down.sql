BEGIN;

ALTER TABLE IF EXISTS patient_identifier
    DROP COLUMN IF EXISTS assigner_id;

COMMIT;