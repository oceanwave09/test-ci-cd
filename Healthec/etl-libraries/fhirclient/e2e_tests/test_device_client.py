# coding: utf-8

import json
import logging
import pytest

from fhirclient.client import FHIRClient
from fhirclient.resources.device import Device
from e2e_tests import base

logging.basicConfig(level=logging.INFO)


class TestDeviceClient:

    @pytest.fixture
    def new_resource(self) -> str:
        with open("e2e_tests/data/device.json", "r") as f:
                content = f.read()
        return content

    @pytest.fixture
    def update_field(self) -> dict:
        with open("e2e_tests/data/device_status.json", "r") as f:
            return json.load(f)

    @classmethod
    def setup_method(cls):
        config = base.get_e2e_tests_config()
        fhir_client = FHIRClient(config)
        cls.client = Device(fhir_client)

    # TODO: skips test since the device create api endpoint has not exposed
    @pytest.mark.skip()
    def test_create_device(self, new_resource):
        resp_data = self.client.create(new_resource)
        assert resp_data is not None
        _id = resp_data.get("id")
        assert _id is not None
    
    # TODO: skips test since the device create api endpoint has not exposed
    @pytest.mark.skip()
    def test_get_device(self, new_resource):
        # create resource
        resp_data = self.client.create(new_resource)
        assert resp_data is not None
        _id = resp_data.get("id")
        assert _id is not None

        # test get resource
        resp_data = self.client.get(_id)
        assert resp_data is not None
        assert resp_data.get("id") == _id

    # TODO: skips test since the device create api endpoint has not exposed
    @pytest.mark.skip()
    def test_update_device(self, new_resource, update_field):
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

    # TODO: skips test since the device create api endpoint has not exposed
    @pytest.mark.skip()
    def test_match_device(self, new_resource):
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
