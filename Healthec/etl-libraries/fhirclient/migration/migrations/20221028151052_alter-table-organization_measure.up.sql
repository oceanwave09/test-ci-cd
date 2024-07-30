BEGIN;

ALTER TABLE IF EXISTS organization_measure
    DROP COLUMN IF EXISTS measure_version,
    ADD COLUMN IF NOT EXISTS measure_year SMALLINT NOT NULL DEFAULT 2022,
    DROP CONSTRAINT IF EXISTS organization_measure_org_id_measure_id_measure_year_key,
    ADD CONSTRAINT organization_measure_org_id_measure_id_measure_year_key UNIQUE(organization_id, measure_id, measure_year);

ALTER TABLE IF EXISTS organization_measure_history
    DROP COLUMN IF EXISTS measure_version,
    ADD COLUMN IF NOT EXISTS measure_year SMALLINT NOT NULL DEFAULT 2022;

COMMIT;

END;