import base64
import logging

import requests
from kubernetes import client, config

from providerdags.utils.auth import AuthClient
from providerdags.utils.constants import (
    DATA_KEY_NAMESPACE,
    EVENT_SERVICE_CONFIG_NAME,
    EVENT_SERVICE_CONFIG_NAMESPACE,
    EVENT_SERVICE_SECRET_NAME,
    EVENT_SERVICE_SECRET_NAMESPACE,
    RESPONSE_CODE_201,
)
from providerdags.utils.models import EventMessage


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


def get_data_key(tenant: str) -> str:
    data_key_secret = _get_k8s_secret(f"{tenant}-data-key", DATA_KEY_NAMESPACE)
    pipeline_data_key = base64.b64decode(data_key_secret.get("pipeline_data_key", "")).decode("utf-8")
    return pipeline_data_key
