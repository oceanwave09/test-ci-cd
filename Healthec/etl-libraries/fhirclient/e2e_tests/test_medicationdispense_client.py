# coding: utf-8

from string import Template
import json
import logging
import pytest

from fhirclient.client import FHIRClient
from fhirclient.resources.organization import Organization
from fhirclient.resources.patient import Patient
from fhirclient.resources.encounter import Encounter
from fhirclient.resources.medicationrequest import MedicationRequest
from fhirclient.resources.medicationdispense import MedicationDispense
from e2e_tests import base, utils

logging.basicConfig(level=logging.INFO)


class TestMedicationDispenseClient:

    @pytest.fixture
    def new_resource(self) -> str:
        with open("e2e_tests/data/medicationdispense.json", "r") as f:
                template = Template(f.read())
        values = {}
        values["pat_id"] = self.pat_id
        values["enc_id"] = self.enc_id
        values["med_req_id"] = self.med_req_id
        return template.safe_substitute(values)

    @pytest.fixture
    def update_field(self) -> dict:
        with open("e2e_tests/data/medicationdispense_status.json", "r") as f:
            return json.loads(f.read())

    @classmethod
    def setup_method(cls):
        config = base.get_e2e_tests_config()
        fhir_client = FHIRClient(config)
        # create organization resource
        org_client = Organization(fhir_client)
        with open("e2e_tests/data/organization.json", "r") as f:
            template = Template(f.read())
        values = {}
        values["tax_id"] = utils.random_integer_by_size(9)
        new_org_resource = template.safe_substitute(values)
        resp_data = org_client.create(new_org_resource)
        org_id = resp_data.get("id")
        cls.org_id = org_id
        # create patient resource
        pat_client = Patient(fhir_client)
        with open("e2e_tests/data/patient.json", "r") as f:
            template = Template(f.read())
        values = {}
        values["ssn"] = utils.random_integer_by_size(9)
        values["org_id"] = org_id
        new_pat_resource = template.safe_substitute(values)
        resp_data = pat_client.create(new_pat_resource)
        pat_id = resp_data.get("id")
        cls.pat_id = pat_id
        # create encounter resource
        enc_client = Encounter(fhir_client)
        with open("e2e_tests/data/encounter.json", "r") as f:
            template = Template(f.read())
        values = {}
        values["pat_id"] = pat_id
        values["org_id"] = org_id
        new_enc_resource = template.safe_substitute(values)
        resp_data = enc_client.create(new_enc_resource)
        enc_id = resp_data.get("id")
        cls.enc_id = enc_id
        # create medicationrequest resource
        med_req_client = MedicationRequest(fhir_client)
        with open("e2e_tests/data/medicationrequest.json", "r") as f:
            template = Template(f.read())
        values = {}
        values["pat_id"] = pat_id
        values["enc_id"] = enc_id
        new_med_req_resource = template.safe_substitute(values)
        resp_data = med_req_client.create(new_med_req_resource)
        med_req_id = resp_data.get("id")
        cls.med_req_id = med_req_id
        # create medicationdispense client
        cls.client = MedicationDispense(fhir_client)

    # TODO: skips test since the medication dispense create api endpoint has not exposed
    # @pytest.mark.skip()
    def test_create_medicationdispense(self, new_resource):
        resp_data = self.client.create(new_resource)
        assert resp_data is not None
        _id = resp_data.get("id")
        assert _id is not None

    # TODO: skips test since the medication dispense get api endpoint has not exposed
    # @pytest.mark.skip()
    def test_get_medicationdispense(self, new_resource):
        # create resource
        resp_data = self.client.create(new_resource)
        assert resp_data is not None
        _id = resp_data.get("id")
        assert _id is not None

        # test get resource
        resp_data = self.client.get(_id)
        assert resp_data is not None
        assert resp_data.get("id") == _id

    # TODO: skips test since the medication dispense update api endpoint has not exposed
    # @pytest.mark.skip()
    def test_update_medicationdispense(self, new_resource, update_field):
        # create resource
        resp_data = self.client.create(new_resource)
        assert resp_data is not None
        _id = resp_data.get("id")
        assert _id is not None

        # test update resource
        create_resource = resp_data.get("attributes")
        update_resource = {**create_resource, **update_field}
        resp_data = self.client.update(_id, json.dumps(update_resource))
        assert resp_data == True

    # TODO: skips test since the medication dispense match api endpoint has not exposed
    @pytest.mark.skip()
    def test_match_medicationdispense(self, new_resource):
        # create resource
        resp_data = self.client.create(new_resource)
        assert resp_data is not None
        _id = resp_data.get("id")
        assert _id is not None

        # test match resource
        create_resource = json.loads(new_resource)
        identifier = create_resource.get("identifier")[0]
        resp_data = self.client.match(json.dumps({"identifier": [identifier]}))
        assert resp_data is not None
        assert len(resp_data) == 1
        assert resp_data[0].get("id") == _id
