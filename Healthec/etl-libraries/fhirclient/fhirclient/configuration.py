# coding: utf-8


class Configuration(object):
    """FHIR API client configuration"""

    def __init__(
        self,
        service_host: str,
        provider_host: str = None,
        user_host: str = None,
        tenant_subdomain: str = "",
        tenant_id: str = "",
        protocol: str = "http",
        auth_config: dict = {},
        resolve_reference: bool = False,
        financial_host: str = None,
        medication_host: str = None
    ) -> None:
        self._protocol = protocol
        self._service_host = service_host
        self._provider_host = service_host
        self._user_host = service_host
        self._financial_host = service_host
        self._medication_host = service_host
        if provider_host:
            self._provider_host = provider_host
        if user_host:
            self._user_host = user_host
        if financial_host:
            self._financial_host = financial_host
        if medication_host:
            self._medication_host = medication_host
        self._tenant_subdomain = tenant_subdomain
        self._tenant_id = tenant_id
        self._auth_config = auth_config
        self._resolve_reference = resolve_reference

    @classmethod
    def get_default(cls):
        default_auth_config = {
            "auth_disabled": True,
            "auth_host": "development.healthec.com",
            "auth_subdomain": "keycloak",
            "auth_tenant_subdomain": "hec",
        }
        return cls("127.0.0.1:5001", auth_config=default_auth_config)

    @property
    def protocol(self):
        return self._protocol

    @protocol.setter
    def protocol(self, value: str):
        self._protocol = value

    @property
    def service_host(self):
        return self._service_host

    @service_host.setter
    def service_host(self, value: str):
        self._service_host = value

    @property
    def provider_host(self):
        return self._provider_host

    @provider_host.setter
    def provider_host(self, value: str):
        self._provider_host = value

    @property
    def user_host(self):
        return self._user_host

    @user_host.setter
    def user_host(self, value: str):
        self._user_host = value

    @property
    def financial_host(self):
        return self._financial_host

    @financial_host.setter
    def financial_host(self, value: str):
        self._financial_host = value

    @property
    def medication_host(self):
        return self._medication_host

    @medication_host.setter
    def medication_host(self, value: str):
        self._medication_host = value

    @property
    def tenant_subdomain(self):
        return self._tenant_subdomain

    @tenant_subdomain.setter
    def tenant_subdomain(self, value: str):
        self._tenant_subdomain = value

    @property
    def tenant_id(self):
        return self._tenant_id

    @tenant_id.setter
    def tenant_id(self, value: str):
        self._tenant_id = value

    @property
    def auth_config(self):
        return self._auth_config

    @auth_config.setter
    def auth_config(self, value: dict):
        self._auth_config = value
