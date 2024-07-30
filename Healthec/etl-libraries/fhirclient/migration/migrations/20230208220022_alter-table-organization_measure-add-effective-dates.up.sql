BEGIN;

ALTER TABLE IF EXISTS organization_measure
    ADD COLUMN IF NOT EXISTS measure_period_start TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS measure_period_end TIMESTAMP WITH TIME ZONE DEFAULT NULL;

ALTER TABLE IF EXISTS organization_measure_history
    ADD COLUMN IF NOT EXISTS measure_period_start TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS measure_period_end TIMESTAMP WITH TIME ZONE DEFAULT NULL;

COMMIT;

END;