BEGIN;

DROP TABLE IF EXISTS medication_administration;
DROP TABLE IF EXISTS medication_administration_history;

DROP TABLE IF EXISTS medication_statement;
DROP TABLE IF EXISTS medication_statement_history;

DROP TABLE IF EXISTS medication;
DROP TABLE IF EXISTS medication_history;

DROP TABLE IF EXISTS allergy_intolerance;
DROP TABLE IF EXISTS allergy_intolerance_history;

DROP TABLE IF EXISTS care_plan;

DROP TABLE IF EXISTS condition;
DROP TABLE IF EXISTS condition_history;

DROP TABLE IF EXISTS immunization;
DROP TABLE IF EXISTS immunization_history;

DROP TABLE IF EXISTS intervention;

DROP TABLE IF EXISTS lab_result;

DROP TABLE IF EXISTS procedure;
DROP TABLE IF EXISTS procedure_history;

DROP TABLE IF EXISTS observation;
DROP TABLE IF EXISTS observation_history;

DROP TABLE IF EXISTS encounter;
DROP TABLE IF EXISTS encounter_history;

DROP TABLE IF EXISTS episode_of_care;
DROP TABLE IF EXISTS episode_of_care_history;

DROP TABLE IF EXISTS device;
DROP TABLE IF EXISTS device_history;

DROP TABLE IF EXISTS patient_history;

DROP TABLE IF EXISTS patient;


COMMIT;
