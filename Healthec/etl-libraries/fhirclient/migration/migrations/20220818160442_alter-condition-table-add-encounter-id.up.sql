BEGIN;

ALTER TABLE IF EXISTS condition
    ADD COLUMN IF NOT EXISTS encounter_id BIGINT DEFAULT NULL,
    ADD CONSTRAINT encounter_id_fkey FOREIGN KEY (encounter_id) REFERENCES encounter(id);

COMMIT;

END;