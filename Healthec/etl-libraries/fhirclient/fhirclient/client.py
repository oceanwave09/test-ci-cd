# coding: utf-8

import os
import logging
from datetime import datetime, timezone, timedelta

from fhirclient.configuration import Configuration
from fhirclient.auth import AuthClient
from fhirclient.constants import REFRESH_INTERVAL

logger = logging.getLogger(__name__)



class FHIRClient(object):
    """FHIR API client library"""

    def __init__(
        self,
        configuration: Configuration = None,
        client_id: str = None,
        client_secret: str = None,
    ) -> None:
        if configuration is None:
            configuration = Configuration.get_default()
        logger.debug("Creating FHIR client with configuration %s", configuration)
        self._configuration = configuration

        self._service_base_url = self._get_service_base_url(
            self._configuration._service_host
        )
        self._provider_base_url = self._get_service_base_url(
            self._configuration._provider_host
        )
        self._user_base_url = self._get_service_base_url(self._configuration._user_host)
        self._financial_base_url = self._get_service_base_url(
            self._configuration._financial_host
        )
        self._medication_base_url = self._get_service_base_url(
            self._configuration._medication_host
        )
        logger.debug("Patient base url: %s", self._service_base_url)
        logger.debug("Provider base url: %s", self._provider_base_url)
        logger.debug("User base url: %s", self._user_base_url)
        logger.debug("Financial base url: %s", self._financial_base_url)
        logger.debug("Medication base url: %s", self._medication_base_url)


        auth_disabled = self._configuration.auth_config.get("auth_disabled", False)
        self._auth_disabled = os.getenv("FHIR_CLIENT_AUTH_DISABLED", auth_disabled)

        self._resolve_reference = self._configuration._resolve_reference

        if not self._auth_disabled:
            self._auth_client = AuthClient(self._configuration.auth_config)
            self._client_id = os.getenv("FHIR_CLIENT_ID", client_id)
            self._client_secret = os.getenv("FHIR_CLIENT_SECRET", client_secret)
            self._auth_bearer_token = None
            self._auth_token_expires_in = None
            self._auth_refresh_interval = REFRESH_INTERVAL
        else:
            logger.warning("Auth disabled")

    @property
    def configuration(self) -> Configuration:
        return self._configuration

    @classmethod
    def new_client(
        cls,
        configuration: Configuration = None,
        client_id: str = None,
        client_secret: str = None,
    ):
        return cls(configuration, client_id, client_secret)

    def ready(self) -> bool:
        if self._auth_disabled or (
            self._auth_bearer_token
            and self._auth_token_expires_in > datetime.now(timezone.utc)
        ):
            return True
        return False

    def prepare(self) -> None:
        if not self.ready():
            logger.info("Client is not ready")
            access_token, expires_in = self._auth_client.generate_access_token(
                self._client_id, self._client_secret
            )
            self._auth_bearer_token = f"Bearer {access_token}"
            self._auth_token_expires_in = (
                datetime.now(timezone.utc)
                + timedelta(seconds=expires_in)
                - timedelta(seconds=self._auth_refresh_interval)
            )

    def _get_service_base_url(self, host: str) -> str:
        if self._configuration.tenant_subdomain:
            return (
                f"{self._configuration.protocol}://"
                f"{self._configuration.tenant_subdomain}.{host}"
                f"/api/"
            )
        elif self._configuration.tenant_id:
            return (
                f"{self._configuration.protocol}://"
                f"{host}/{self._configuration.tenant_id}/"
            )
        else:
            return f"{self._configuration.protocol}://{host}/api/"
