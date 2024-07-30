BEGIN;

ALTER TABLE IF EXISTS coverage
    ADD COLUMN IF NOT EXISTS insurance_plan_id BIGINT,
    ADD CONSTRAINT coverage_insurance_plan_id_fkey FOREIGN KEY (insurance_plan_id) REFERENCES insurance_plan(id);

ALTER TABLE IF EXISTS coverage
    ADD COLUMN IF NOT EXISTS insurer_id UUID DEFAULT NULL;

ALTER TABLE IF EXISTS coverage
    DROP COLUMN IF EXISTS payor_id;

COMMIT;

END;