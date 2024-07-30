import base64
import json
import logging

import requests
import simplejson
from airflow.exceptions import AirflowFailException
from fhirclient.client import FHIRClient
from fhirclient.configuration import Configuration
from fhirclient.resources.base import Base
from kubernetes import client, config

from patientdags.utils.auth import AuthClient
from patientdags.utils.constants import (
    EVENT_SERVICE_CONFIG_NAME,
    EVENT_SERVICE_CONFIG_NAMESPACE,
    EVENT_SERVICE_SECRET_NAME,
    EVENT_SERVICE_SECRET_NAMESPACE,
    MEASURE_CONNECTION,
    PLATFORM_SERVICE_CONFIG_NAME_SUFFIX,
    PLATFORM_SERVICE_CONFIG_NAMESPACE,
    PLATFORM_SERVICE_SECRET_NAME_SUFFIX,
    PLATFORM_SERVICE_SECRET_NAMESPACE,
    RESPONSE_CODE_200,
    RESPONSE_CODE_201,
    RESPONSE_CODE_409,
)
from patientdags.utils.error_codes import PatientDagsErrorCodes, publish_error_code
from patientdags.utils.models import EventMessage


def _get_fhir_client(tenant: str):
    plaform_service_config_name = f"{tenant.lower()}-{PLATFORM_SERVICE_CONFIG_NAME_SUFFIX}"
    plaform_service_secret_name = f"{tenant.lower()}-{PLATFORM_SERVICE_SECRET_NAME_SUFFIX}"

    platform_service_config = _get_k8s_configmap(plaform_service_config_name, PLATFORM_SERVICE_CONFIG_NAMESPACE)
    platform_service_creds = _get_k8s_secret(plaform_service_secret_name, PLATFORM_SERVICE_SECRET_NAMESPACE)
    platform_auth_client_id = base64.b64decode(platform_service_creds.get("service_auth_client_id", "")).decode("utf-8")
    platform_auth_client_secret = base64.b64decode(platform_service_creds.get("service_auth_client_secret", "")).decode(
        "utf-8"
    )

    auth_config = {
        "protocol": platform_service_config.get("service_auth_protocol", "https"),
        "auth_host": platform_service_config.get("service_auth_host", ""),
        "auth_subdomain": platform_service_config.get("service_auth_subdomain", ""),
        "auth_tenant_subdomain": platform_service_config.get("service_auth_tenant_subdomain", ""),
    }

    client_config = Configuration(
        protocol=platform_service_config.get("service_protocol", "https"),
        service_host=platform_service_config.get("patient_host", ""),
        provider_host=platform_service_config.get("provider_host", ""),
        user_host=platform_service_config.get("user_host", ""),
        financial_host=platform_service_config.get("financial_host", ""),
        medication_host=platform_service_config.get("medication_host", ""),
        tenant_subdomain=platform_service_config.get("service_tenant_subdomain", ""),
        tenant_id=platform_service_config.get("service_tenant_id", ""),
        auth_config=auth_config,
    )
    return FHIRClient(client_config, client_id=platform_auth_client_id, client_secret=platform_auth_client_secret)


def _get_k8s_configmap(name: str, namespace: str = "data-pipeline") -> dict:
    v1 = client.CoreV1Api(client.ApiClient(configuration=config.load_incluster_config()))
    response = v1.read_namespaced_config_map(name, namespace)
    return response.data


def _get_k8s_secret(name: str, namespace: str = "data-pipeline") -> dict:
    v1 = client.CoreV1Api(client.ApiClient(configuration=config.load_incluster_config()))
    response = v1.read_namespaced_secret(name, namespace)
    return response.data


def _get_service_domain(service_config: dict) -> str:
    protocol = service_config.get("service_protocol", "https")
    host = service_config.get("service_host", "")
    tenant_subdomain = service_config.get("service_tenant_subdomain", "")
    tenant_id = service_config.get("service_tenant_id", "")
    if tenant_id:
        return f"{protocol}://{host}/{tenant_id}"
    else:
        return f"{protocol}://{tenant_subdomain}.{host}/api"


def post_resource_entity(tenant: str, resource: dict, entity=Base, scope: str = None, scope_id: str = None):
    try:
        fhir_client = _get_fhir_client(tenant)
        if scope_id:
            logging.info(f"scope: {scope}, scope_id: {scope_id}")
            entity_client = entity(client=fhir_client, scope=scope, scope_id=scope_id)
        else:
            entity_client = entity(client=fhir_client)
        payload = simplejson.dumps(resource)
        # logging.debug(f"post request payload: {payload}")
        response = entity_client.create(payload)
        resource_id = response.get("id")
        logging.info(f"post resource id: {resource_id}")
        return resource_id
    except Exception as e:
        # handling resource already exists `status code - 409`
        if str(RESPONSE_CODE_409) in str(e):
            logging.warn(str(e))
            return
        publish_error_code(f"{PatientDagsErrorCodes.API_CLIENT_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def upsert_resource_entity(tenant: str, resource: dict, entity=Base, scope: str = None, scope_id: str = None):
    try:
        fhir_client = _get_fhir_client(tenant)
        if scope_id:
            logging.info(f"scope: {scope}, scope_id: {scope_id}")
            entity_client = entity(client=fhir_client, scope=scope, scope_id=scope_id)
        else:
            entity_client = entity(client=fhir_client)
        payload = simplejson.dumps(resource)
        # logging.debug(f"upsert request payload: {payload}")
        response = entity_client.upsert(payload)
        resource_id = response.get("id")
        logging.info(f"upsert resource id: {resource_id}")
        return resource_id
    except Exception as e:
        publish_error_code(f"{PatientDagsErrorCodes.API_CLIENT_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def match_resource_entity(tenant: str, resource: dict, entity=Base, scope: str = None, scope_id: str = None):
    try:
        fhir_client = _get_fhir_client(tenant)
        if scope_id:
            logging.info(f"scope: {scope}, scope_id: {scope_id}")
            entity_client = entity(client=fhir_client, scope=scope, scope_id=scope_id)
        else:
            entity_client = entity(client=fhir_client)
        payload = simplejson.dumps(resource)
        # logging.debug(f"match request payload: {payload}")
        matches = entity_client.match(payload)
        logging.info(f"match response: {matches}")
        if matches:
            match_id = matches[0].get("id")
            return match_id
        return None
    except Exception as e:
        publish_error_code(f"{PatientDagsErrorCodes.API_CLIENT_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def get_resource_entity(tenant: str, resource_id: str, entity=Base, scope: str = None, scope_id: str = None):
    try:
        fhir_client = _get_fhir_client(tenant)
        if scope_id:
            logging.info(f"scope: {scope}, scope_id: {scope_id}")
            entity_client = entity(client=fhir_client, scope=scope, scope_id=scope_id)
        else:
            entity_client = entity(client=fhir_client)
        logging.info(f"get request resource id: {resource_id}")
        response = entity_client.get(resource_id)
        resource = None
        if response.get("id", "") == resource_id:
            resource = response.get("attributes", {})
        # logging.debug(f"match resource: {resource}")
        return resource
    except Exception as e:
        publish_error_code(f"{PatientDagsErrorCodes.API_CLIENT_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def put_resource_entity(
    tenant: str, resource_id: str, resource: dict, entity=Base, scope: str = None, scope_id: str = None
):
    try:
        fhir_client = _get_fhir_client(tenant)
        if scope_id:
            logging.info(f"scope: {scope}, scope_id: {scope_id}")
            entity_client = entity(client=fhir_client, scope=scope, scope_id=scope_id)
        else:
            entity_client = entity(client=fhir_client)
        payload = json.dumps(resource)
        logging.info(f"put request resource id: {resource_id}")
        # logging.debug(f"put request payload: {payload}")
        response = entity_client.update(resource_id, payload)
        # logging.debug(f"put response: {response}")
        return response
    except Exception as e:
        publish_error_code(f"{PatientDagsErrorCodes.API_CLIENT_ERROR.value}: {str(e)}")
        raise AirflowFailException(e)


def _get_measure_url():
    protocol = MEASURE_CONNECTION.extra_dejson.get("protocol")
    host = MEASURE_CONNECTION.host
    tenant_subdomain = MEASURE_CONNECTION.extra_dejson.get("tenant_subdomain")
    # measure_start_date = MEASURE_CONNECTION.extra_dejson.get("start_date")
    # return f"{protocol}://{tenant_subdomain}.{host}/api/measures/execute/{measure_start_date}"
    return f"{protocol}://{tenant_subdomain}.{host}/api/measures/execute"


def calculate_patient_measure(patient_id: str, bearer_token: str) -> bool:
    url = _get_measure_url()
    headers = {"Content-Type": "application/json", "Authorization": bearer_token}
    tenant = MEASURE_CONNECTION.extra_dejson.get("tenant_subdomain")
    payload = json.dumps(
        {
            "bundle": f"http://patient-service:5000/{tenant}/bundles/patients/{patient_id}/",
            "patientId": f"{patient_id}",
        }
    )
    response = requests.post(url, headers=headers, data=payload, verify=False)
    if response.status_code != RESPONSE_CODE_200:
        logging.error(f"Measures triggered failed for patient {patient_id}, status code: {response.status_code}")
        return False
    return True


def post_event_message(message: EventMessage) -> bool:
    event_service_config = _get_k8s_configmap(EVENT_SERVICE_CONFIG_NAME, EVENT_SERVICE_CONFIG_NAMESPACE)
    event_service_creds = _get_k8s_secret(EVENT_SERVICE_SECRET_NAME, EVENT_SERVICE_SECRET_NAMESPACE)
    event_auth_client_id = base64.b64decode(event_service_creds.get("service_auth_client_id", "")).decode("utf-8")
    event_auth_client_secret = base64.b64decode(event_service_creds.get("service_auth_client_secret", "")).decode(
        "utf-8"
    )

    auth_client = AuthClient(
        protocol=event_service_config.get("service_auth_protocol", ""),
        host=event_service_config.get("service_auth_host", ""),
        subdomain=event_service_config.get("service_auth_subdomain", ""),
        tenant_subdomain=event_service_config.get("service_auth_tenant_subdomain", ""),
    )
    bearer_token = auth_client.get_access_token(event_auth_client_id, event_auth_client_secret)
    url = f"{_get_service_domain(event_service_config)}/events/"
    headers = {"Content-Type": "application/json", "Authorization": bearer_token}
    response = requests.post(url, headers=headers, data=str(message), verify=False)
    if response.status_code != RESPONSE_CODE_201:
        logging.error(
            f"Posting file processing audit event failed, "
            f"url: {url} "
            f"error: {response.status_code} - {response.reason}"
        )
        return False
    logging.info(f"File processing audit event posted successfully, {response.text}")
    return True
