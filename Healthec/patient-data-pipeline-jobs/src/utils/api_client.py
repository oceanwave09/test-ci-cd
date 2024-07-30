import json
import logging
import os
from datetime import datetime

import requests
from fhirclient.auth import AuthClient
from fhirclient.client import FHIRClient
from fhirclient.configuration import Configuration
from fhirclient.resources.base import Base

from utils.constants import RESPONSE_CODE_200, RESPONSE_CODE_201
from utils.models import EventMessage

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
    return FHIRClient(
        client_config,
        client_id=auth_client_id,
        client_secret=auth_client_secret,
    )


def get_core_entity(resource_id: str, entity: Base) -> str:
    logging.info(f"get resource with resource id {resource_id}")
    fhir_client = _get_fhir_client()
    entity_client = entity(client=fhir_client)

    data = entity_client.get(resource_id)
    resource = data.get("attributes", {})

    return json.dumps(resource)


def get_patient_sub_entities(
    resource_id: str,
    patient_id: str,
    entity: Base,
) -> str:
    logging.info(f"get {entity} resource with resource id {resource_id}")
    fhir_client = _get_fhir_client()
    entity_client = entity(
        client=fhir_client,
        scope="Patient",
        scope_id=patient_id,
    )

    data = entity_client.get(resource_id)
    resource = data.get("attributes", {})

    return json.dumps(resource)


def post_core_entity(resource: str, entity: Base) -> str:
    logging.info(f"post resource: {resource}")
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
    return data[0].get("id") if data and len(data) > 0 else None


def post_sub_entity(resource: str, entity: Base, scope_id: str, scope: str = "Patient") -> str:
    logging.info(f"post resource:  {resource}")
    fhir_client = _get_fhir_client()
    entity_client = entity(client=fhir_client, scope=scope, scope_id=scope_id)

    try:
        data = entity_client.create(resource)
        return data.get("id")
    except Exception as error:
        return f"failure: {str(error)}"


def match_sub_entity(attributes: str, entity: Base, scope_id: str, scope: str = "Patient") -> str:
    logging.info(f"match attributes:  {attributes}")
    fhir_client = _get_fhir_client()
    entity_client = entity(client=fhir_client, scope=scope, scope_id=scope_id)

    data = entity_client.match(attributes)
    return data[0].get("id") if data and len(data) > 0 else None


def _get_patient_measure_url(organization_id: str, measure_id: str, patient_id: str, date: str = None) -> str:
    protocol = os.environ.get("PLATFORM_SERVICE_PROTOCOL", DEFAULT_PROTOCOL)
    host = os.environ.get("PLATFORM_MEASURE_HOST", "")
    tenant_subdomain = os.environ.get("PLATFORM_SERVICE_TENANT_SUBDOMAIN", "")
    tenant_id = os.environ.get("PLATFORM_SERVICE_TENANT_ID", "")
    if not date:
        date = datetime.strftime(datetime.utcnow().date().replace(month=12, day=31), "")
    if tenant_id:
        url = (
            f"{protocol}://{host}/{tenant_id}/measures/"
            f"organizations/{organization_id}/measures/{measure_id}/"
            f"{date}/patients/{patient_id}"
        )
    else:
        url = (
            f"{protocol}://{tenant_subdomain}.{host}/api/measures/"
            f"organizations/{organization_id}/measures/{measure_id}/"
            f"{date}/patients/{patient_id}"
        )
    return url


def get_measure(organization_id: str, measure_id: str, patient_id: str, date: str = None):
    url = _get_patient_measure_url(organization_id, measure_id, patient_id, date)
    auth_client = AuthClient(_get_auth_config())
    auth_client_id = os.environ.get("PLATFORM_SERVICE_AUTH_CLIENT_ID")
    auth_client_secret = os.environ.get("PLATFORM_SERVICE_AUTH_CLIENT_SECRET")
    access_details = auth_client.generate_access_token(auth_client_id, auth_client_secret)
    bearer_token = access_details[0] if access_details and len(access_details) > 0 else ""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {bearer_token}",
    }
    try:
        response = requests.get(url, headers=headers, verify=False)
        if response.status_code != RESPONSE_CODE_200:
            logging.error(f"Failed to fetch measure values, status code: {response.status_code}")
        return response.json()
    except Exception as e:
        logging.error(f"Failed to fetch measure values. Error: {str(e)}")
        return {}


def _get_event_service_domain() -> str:
    protocol = os.environ.get("EVENT_SERVICE_PROTOCOL", DEFAULT_PROTOCOL)
    host = os.environ.get("EVENT_SERVICE_HOST", "")
    tenant_subdomain = os.environ.get("EVENT_SERVICE_TENANT_SUBDOMAIN", "")
    tenant_id = os.environ.get("EVENT_SERVICE_TENANT_ID", "")
    if tenant_id:
        return f"{protocol}://{host}/{tenant_id}"
    else:
        return f"{protocol}://{tenant_subdomain}.{host}/api"


def post_event_message(message: EventMessage) -> bool:
    event_auth_client_id = os.environ.get("EVENT_SERVICE_AUTH_CLIENT_ID", "")
    event_auth_client_secret = os.environ.get("EVENT_SERVICE_AUTH_CLIENT_SECRET", "")

    auth_client = AuthClient(
        auth_config={
            "protocol": os.environ.get("EVENT_SERVICE_AUTH_PROTOCOL", DEFAULT_PROTOCOL),
            "auth_host": os.environ.get("EVENT_SERVICE_AUTH_HOST", ""),
            "auth_subdomain": os.environ.get("EVENT_SERVICE_AUTH_SUBDOMAIN", ""),
            "auth_tenant_subdomain": os.environ.get("EVENT_SERVICE_AUTH_TENANT_SUBDOMAIN", ""),
        }
    )
    access_details = auth_client.generate_access_token(event_auth_client_id, event_auth_client_secret)
    bearer_token = access_details[0] if access_details and len(access_details) > 0 else ""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {bearer_token}",
    }
    url = f"{_get_event_service_domain()}/events/"
    response = requests.post(url, headers=headers, data=str(message), verify=False)
    if response.status_code != RESPONSE_CODE_201:
        logging.error(
            f"Posting file processing audit event failed, "
            f"url: {url} "
            f"error: {response.status_code} - {response.reason}"
        )
        return False
    logging.warn(f"File processing audit event posted successfully, {response.text}")
    return True
