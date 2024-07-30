# coding: utf-8

from typing import Tuple
import requests
import logging

from fhirclient.constants import RESPONSE_CODE_200
from fhirclient.utils import is_null_or_empty
from fhirclient.exceptions import ApiValueError, ApiException

logger = logging.getLogger(__name__)


class AuthClient(object):
    """FHIR auth client"""

    def __init__(self, auth_config: dict) -> None:
        self._protocol = auth_config.get("protocol", "https")

        self._auth_config = auth_config
        if is_null_or_empty(auth_config.get("auth_host")):
            raise ApiValueError("Auth host should not be empty or null")
        self._host = auth_config.get("auth_host")

        self._subdomain = auth_config.get("auth_subdomain") if auth_config.get("auth_subdomain") else ""

        if is_null_or_empty(auth_config.get("auth_tenant_subdomain")):
            raise ApiValueError("Auth tenant subdomain should not be empty or null")
        self._tenant_subdomain = auth_config.get("auth_tenant_subdomain")

    @property
    def auth_config(self) -> dict:
        return self._auth_config

    def generate_access_token(self, client_id: str, client_secret: str) -> Tuple:
        """Generates access token"""
        logger.info("Generating access token")
        if is_null_or_empty(client_id):
            raise ApiValueError("Client id should not be empty or null")
        if is_null_or_empty(client_secret):
            raise ApiValueError("Client secret should not be empty or null")

        if self._subdomain:
            auth_url = f"{self._protocol}://{self._subdomain}.{self._host}/realms/{self._tenant_subdomain}/protocol/openid-connect/token"
        else:
            auth_url = f"{self._protocol}://{self._host}/realms/{self._tenant_subdomain}/protocol/openid-connect/token"
        data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
            "scope": "openid",
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        try:
            response = requests.post(auth_url, headers=headers, data=data, verify=False)
            if response and response.status_code == RESPONSE_CODE_200:
                auth = response.json()
                return auth.get("access_token"), auth.get("expires_in")
            else:
                raise ApiException(
                    "Failed to get auth access token",
                    f"{str(response.status_code)}, {response.reason}",
                )
        except requests.RequestException as e:
            raise ApiException("Failed to get auth access token", str(e))
