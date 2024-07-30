import json
import logging
import os

from fhirclient.client import FHIRClient
from fhirclient.configuration import Configuration
from fhirclient.resources.base import Base

DEFAULT_PROTOCOL = "https"


def _get_auth_config() -> dict:
    return {
        "protocol": os.environ.get("PLATFORM_SERVICE_AUTH_PROTOCOL", DEFAULT_PROTOCOL),
        "auth_host": os.environ.get("PLATFORM_SERVICE_AUTH_HOST", ""),
        "auth_subdomain": os.environ.get("PLATFORM_SERVICE_AUTH_SUBDOMAIN", ""),
        "auth_tenant_subdomain": os.environ.get("PLATFORM_SERVICE_AUTH_TENANT_SUBDOMAIN", ""),
    }


def _get_fhir_client():
    client_config = Configuration(
        protocol=os.environ.get("PLATFORM_SERVICE_PROTOCOL", DEFAULT_PROTOCOL),
        service_host=os.environ.get("PLATFORM_PATIENT_HOST", ""),
        provider_host=os.environ.get("PLATFORM_PROVIDER_HOST", ""),
        user_host=os.environ.get("PLATFORM_USER_HOST", ""),
        financial_host=os.environ.get("PLATFORM_FINANCIAL_HOST", ""),
        tenant_subdomain=os.environ.get("PLATFORM_SERVICE_TENANT_SUBDOMAIN", ""),
        tenant_id=os.environ.get("PLATFORM_SERVICE_TENANT_ID", ""),
        auth_config=_get_auth_config(),
    )
    auth_client_id = os.environ.get("PLATFORM_SERVICE_AUTH_CLIENT_ID")
    auth_client_secret = os.environ.get("PLATFORM_SERVICE_AUTH_CLIENT_SECRET")
    return FHIRClient(client_config, client_id=auth_client_id, client_secret=auth_client_secret)


def get_core_entity(resource_id: str, entity: Base) -> str:
    logging.info(f"get resource with resource id {resource_id}")
    fhir_client = _get_fhir_client()
    entity_client = entity(client=fhir_client)

    data = entity_client.get(resource_id)
    resource = data.get("attributes", {})

    return json.dumps(resource)


def get_sub_entities(resource_id: str, provider_id: str, entity: Base) -> str:
    logging.info(f"get {entity} resource with resource id {resource_id}")
    fhir_client = _get_fhir_client()
    entity_client = entity(
        client=fhir_client,
        scope="Practitioner",
        scope_id=provider_id,
    )

    data = entity_client.get(resource_id)
    resource = data.get("attributes", {})

    return json.dumps(resource)


def post_core_entity(resource: str, entity: Base) -> str:
    logging.info(f"post resource:  {resource}")
    fhir_client = _get_fhir_client()
    entity_client = entity(client=fhir_client)

    try:
        data = entity_client.create(resource)
        return data.get("id")
    except Exception as error:
        return f"failure: {str(error)}"


def match_core_entity(attributes: str, entity: Base) -> str:
    logging.info(f"match attributes:  {attributes}")
    fhir_client = _get_fhir_client()
    entity_client = entity(client=fhir_client)

    data = entity_client.match(attributes)
    return data[0].get("id") if len(data) > 0 else None


def post_sub_entity(resource: str, entity: Base, scope_id: str, scope: str = "Practitioner") -> str:
    logging.info(f"post resource:  {resource}")
    fhir_client = _get_fhir_client()
    entity_client = entity(client=fhir_client, scope=scope, scope_id=scope_id)

    try:
        data = entity_client.create(resource)
        return data.get("id")
    except Exception as error:
        return f"failure: {str(error)}"
