BEGIN;

ALTER TABLE IF EXISTS patient_identifier
    ADD COLUMN IF NOT EXISTS assigner_id TEXT DEFAULT NULL;

COMMIT;

END;