BEGIN;

ALTER TABLE IF EXISTS patient_link DROP CONSTRAINT IF EXISTS patient_lik_patient_id_linked_patient_id_key;

COMMIT;

END;
