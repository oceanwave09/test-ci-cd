# coding: utf-8

from fhirclient.resources.base import Base
from fhirclient.client import FHIRClient
from fhirclient.constants import ResourceType


class Practitioner(Base):
    """Practitioner resource API client"""

    def __init__(self, client: FHIRClient) -> None:
        super().__init__(
            client,
            ResourceType.Practitioner.value,
            resource_path=ResourceType.Practitioner.get_resource_path(),
        )
