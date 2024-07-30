# coding: utf-8

from fhirclient.configuration import Configuration

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PROTOCOL = "http"

def get_e2e_tests_config() -> Configuration:
    service_host  = f"{DEFAULT_HOST}:5001"
    provider_host = f"{DEFAULT_HOST}:5002"
    user_host = f"{DEFAULT_HOST}:5003"
    financial_host = f"{DEFAULT_HOST}:5004"
    protocol = DEFAULT_PROTOCOL
    auth_config = {"auth_disabled": True}
    config = Configuration(
        protocol=protocol,
        service_host=service_host,
        provider_host=provider_host,
        user_host=user_host,
        financial_host=financial_host,
        auth_config=auth_config,
    )
    return config
