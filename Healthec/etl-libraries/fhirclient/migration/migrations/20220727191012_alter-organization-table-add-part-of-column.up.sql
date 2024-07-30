BEGIN;

ALTER TABLE IF EXISTS organization
    ADD COLUMN IF NOT EXISTS part_of BIGINT,
    ADD COLUMN IF NOT EXISTS highest_part_of BIGINT,
    ADD CONSTRAINT part_of_fkey FOREIGN KEY (part_of) REFERENCES organization(id),
    ADD CONSTRAINT highest_part_of_fkey FOREIGN KEY (highest_part_of) REFERENCES organization(id);

COMMIT;

END;