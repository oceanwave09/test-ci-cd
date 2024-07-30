# coding: utf-8

from fhirclient.resources.base import Base
from fhirclient.client import FHIRClient
from fhirclient.constants import ResourceType


class InsurancePlan(Base):
    """InsurancePlan resource API client"""

    def __init__(self, client: FHIRClient) -> None:
        super().__init__(
            client,
            ResourceType.InsurancePlan.value,
            resource_path=ResourceType.InsurancePlan.get_resource_path(),
        )
