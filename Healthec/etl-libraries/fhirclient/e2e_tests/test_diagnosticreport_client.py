# coding: utf-8

from string import Template
import json
import logging
import pytest

from fhirclient.client import FHIRClient
from fhirclient.resources.organization import Organization
from fhirclient.resources.patient import Patient
# from fhirclient.resources.practitioner import Practitioner
from fhirclient.resources.observation import Observation
from fhirclient.resources.encounter import Encounter
from fhirclient.resources.diagnosticreport import DiagnosticReport
from e2e_tests import base, utils

logging.basicConfig(level=logging.INFO)


class TestDiagnosticReportClient:

    @pytest.fixture
    def new_resource(self) -> str:
        with open("e2e_tests/data/diagnosticreport.json", "r") as f:
                template = Template(f.read())
        values = {}
        values["pat_id"] = self.pat_id
        # values["pract_id"] = self.pract_id
        values["enc_id"] = self.enc_id
        return template.safe_substitute(values)

    @pytest.fixture
    def update_field(self) -> dict:
        with open("e2e_tests/data/diagnosticreport_status.json", "r") as f:
            template = Template(f.read())
        values = {}
        values["obs_id"] = self.obs_id
        return json.loads(template.safe_substitute(values))

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
        # create pratitioner resource
        # pract_client = Practitioner(fhir_client)
        # with open("e2e_tests/data/practitioner.json", "r") as f:
        #         template = Template(f.read())
        # values = {}
        # values["npi"] = utils.random_integer_by_size(9)
        # new_pract_resource = template.safe_substitute(values)
        # resp_data = pract_client.create(new_pract_resource)
        # pract_id = resp_data.get("id")
        # cls.pract_id = pract_id
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
        # create observation
        obs_client = Observation(fhir_client)
        with open("e2e_tests/data/observation.json", "r") as f:
                template = Template(f.read())
        values = {}
        values["pat_id"] = pat_id
        new_obs_resource = template.safe_substitute(values)
        resp_data = obs_client.create(new_obs_resource)
        obs_id = resp_data.get("id")
        cls.obs_id = obs_id
        # create diagnosticreport client
        cls.client = DiagnosticReport(fhir_client)

    # TODO: skips test since the diagnostic report create api endpoint has not exposed
    # @pytest.mark.skip()
    def test_create_diagnosticreport(self, new_resource):
        resp_data = self.client.create(new_resource)
        assert resp_data is not None
        _id = resp_data.get("id")
        assert _id is not None

    # TODO: skips test since the diagnostic report get api endpoint has not exposed
    # @pytest.mark.skip()
    def test_get_diagnosticreport(self, new_resource):
        # create resource
        resp_data = self.client.create(new_resource)
        assert resp_data is not None
        _id = resp_data.get("id")
        assert _id is not None

        # test get resource
        resp_data = self.client.get(_id)
        assert resp_data is not None
        assert resp_data.get("id") == _id

    # TODO: skips test since the diagnostic report update api endpoint has not exposed
    # @pytest.mark.skip()
    def test_update_diagnosticreport(self, new_resource, update_field):
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

    # TODO: skips test since the diagnostic report match api endpoint has not exposed
    @pytest.mark.skip()
    def test_match_diagnosticreport(self, new_resource):
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
