BEGIN;


ALTER TABLE IF EXISTS patient_link
    ADD CONSTRAINT patient_lik_patient_id_linked_patient_id_key UNIQUE (patient_id, linked_patient_id);

COMMIT;

END;
