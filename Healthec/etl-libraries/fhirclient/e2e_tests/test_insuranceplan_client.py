# coding: utf-8

from string import Template
import json
import logging
import pytest

from fhirclient.client import FHIRClient
from fhirclient.resources.insuranceplan import InsurancePlan
from e2e_tests import base, utils

logging.basicConfig(level=logging.INFO)


class TestInsurancePlanClient:

    @pytest.fixture
    def new_resource(self) -> str:
        with open("e2e_tests/data/insurancePlan.json", "r") as f:
                template = Template(f.read())
        return template.safe_substitute()

    @pytest.fixture
    def update_field(self) -> dict:
        with open("e2e_tests/data/insurancePlan_status.json", "r") as f:
            return json.load(f)

    @classmethod
    def setup(cls):
        config = base.get_e2e_tests_config()
        fhir_client = FHIRClient(config)
        # create Insurance Plan  client
        cls.client = InsurancePlan(fhir_client)

    def test_create_insurance_plan(self, new_resource):
        resp_data = self.client.create(new_resource)
        assert resp_data is not None
        _id = resp_data.get("id")
        assert _id is not None

    def test_get_insurance_plan(self, new_resource):
        # create resource
        resp_data = self.client.create(new_resource)
        assert resp_data is not None
        _id = resp_data.get("id")
        assert _id is not None

        # test get resource
        resp_data = self.client.get(_id)
        assert resp_data is not None
        assert resp_data.get("id") == _id

    def test_update_insurance_plan(self, new_resource, update_field):
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

    def test_match_insurance_plan(self, new_resource):
        # create resource
        resp_data = self.client.create(new_resource)
        assert resp_data is not None
        _id = resp_data.get("id")
        assert _id is not None

        # test match resource
        resource_attributes = resp_data.get("attributes")
        resp_data = self.client.match(json.dumps(resource_attributes))
        assert resp_data is not None
        assert len(resp_data) == 1
        assert resp_data[0].get("id") == _id
