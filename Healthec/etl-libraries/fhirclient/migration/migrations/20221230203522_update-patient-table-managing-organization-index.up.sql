CREATE INDEX IF NOT EXISTS patient_organization_id_idx ON patient((data -> 'managingOrganization' ->> 'id'));
