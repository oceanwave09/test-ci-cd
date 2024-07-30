BEGIN;

ALTER TABLE IF EXISTS organization_measure
    ADD COLUMN IF NOT EXISTS measure_version TEXT,
    DROP COLUMN IF EXISTS measure_year,
    DROP CONSTRAINT IF EXISTS organization_measure_org_id_measure_id_measure_year_key;

ALTER TABLE IF EXISTS organization_measure_history
    ADD COLUMN IF NOT EXISTS measure_version TEXT,
    DROP COLUMN IF EXISTS measure_year;

COMMIT;

END;
