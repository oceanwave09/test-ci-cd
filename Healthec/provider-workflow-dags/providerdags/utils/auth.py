# coding: utf-8

import logging
from datetime import datetime, timedelta, timezone
from typing import Tuple

import requests

from providerdags.utils.constants import RESPONSE_CODE_200

logger = logging.getLogger(__name__)


class AuthClient(object):
    """FHIR auth client"""

    def __init__(self, protocol: str, host: str, subdomain: str, tenant_subdomain: str) -> None:
        # default values
        self._protocol = "https"
        self._host = "development.healthec.com"
        self._subdomain = ""
        self._tenant_subdomain = ""
        self._token = None
        self._token_expires_in = None
        self._auth_refresh_interval = 30

        if protocol:
            self._protocol = protocol
        if host:
            self._host = host
        if subdomain:
            self._subdomain = subdomain
        if tenant_subdomain:
            self._tenant_subdomain = tenant_subdomain

    def _generate_access_token(self, client_id: str, client_secret: str) -> Tuple:
        if self._subdomain:
            auth_url = (
                f"{self._protocol}://{self._subdomain}.{self._host}/realms/{self._tenant_subdomain}"
                f"/protocol/openid-connect/token"
            )
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
                raise Exception(
                    "Failed to get auth access token",
                    f"{str(response.status_code)}, {response.reason}",
                )
        except requests.RequestException as e:
            raise Exception("Failed to get auth access token", str(e))

    def get_access_token(self, client_id: str, client_secret: str):
        if not client_id or not client_secret:
            raise ValueError("client credentials should not be empty or null")

        if self._token is None or self._token_expires_in < datetime.now(timezone.utc):
            access_token, expires_in = self._generate_access_token(client_id, client_secret)
            self._token = f"Bearer {access_token}"
            self._token_expires_in = (
                datetime.now(timezone.utc)
                + timedelta(seconds=expires_in)
                - timedelta(seconds=self._auth_refresh_interval)
            )
        return self._token
