# coding: utf-8

from string import Template
import json
import logging
import pytest

from fhirclient.client import FHIRClient
from fhirclient.resources.organization import Organization
from fhirclient.resources.location import Location
from e2e_tests import base, utils

logging.basicConfig(level=logging.INFO)


class TestLocationClient:

    @pytest.fixture
    def new_resource(self) -> str:
        with open("e2e_tests/data/location.json", "r") as f:
                template = Template(f.read())
        values = {}
        values["org_id"] = self.org_id
        return template.safe_substitute(values)

    @pytest.fixture
    def update_field(self) -> dict:
        with open("e2e_tests/data/location_status.json", "r") as f:
            return json.load(f)

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
        cls.org_id = resp_data.get("id")
        # create location client
        cls.client = Location(fhir_client)

    def test_create_location(self, new_resource):
        resp_data = self.client.create(new_resource)
        assert resp_data is not None
        _id = resp_data.get("id")
        assert _id is not None

    def test_get_location(self, new_resource):
        # create resource
        resp_data = self.client.create(new_resource)
        assert resp_data is not None
        _id = resp_data.get("id")
        assert _id is not None

        # test get resource
        resp_data = self.client.get(_id)
        assert resp_data is not None
        assert resp_data.get("id") == _id

    def test_update_location(self, new_resource, update_field):
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

    # TODO: skips test since the location match field is unknown
    @pytest.mark.skip()
    def test_match_location(self, new_resource):
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
