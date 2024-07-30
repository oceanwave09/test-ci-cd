BEGIN;

ALTER TABLE IF EXISTS procedure
    ADD COLUMN IF NOT EXISTS encounter_id BIGINT, -- NOT NULL would need to have a default value
    ADD CONSTRAINT encounter_id_fkey FOREIGN KEY (encounter_id) REFERENCES encounter(id);

COMMIT;

END;