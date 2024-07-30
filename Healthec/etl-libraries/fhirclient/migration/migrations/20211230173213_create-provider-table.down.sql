BEGIN;

DROP TABLE IF EXISTS organization_measure;
DROP TABLE IF EXISTS organization_measure_history;

DROP TABLE IF EXISTS organization_affiliation;
DROP TABLE IF EXISTS organization_affiliation_history;

DROP TABLE IF EXISTS location;
DROP TABLE IF EXISTS location_history;

DROP TABLE IF EXISTS practitioner_role;
DROP TABLE IF EXISTS practitioner_role_history;

DROP TABLE IF EXISTS organization;
DROP TABLE IF EXISTS organization_history;

DROP TABLE IF EXISTS organization_type;
DROP TABLE IF EXISTS organization_type_history;

DROP TABLE IF EXISTS practitioner;
DROP TABLE IF EXISTS practitioner_history;

COMMIT;
