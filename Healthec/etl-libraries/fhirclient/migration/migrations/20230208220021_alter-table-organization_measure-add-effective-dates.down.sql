BEGIN;

ALTER TABLE IF EXISTS organization_measure
    DROP COLUMN IF EXISTS measure_period_start,
    DROP COLUMN IF EXISTS measure_period_start;

ALTER TABLE IF EXISTS organization_measure_history
    DROP COLUMN IF EXISTS measure_period_start,
    DROP COLUMN IF EXISTS measure_period_start;

COMMIT;