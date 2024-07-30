import json
import logging

import requests
from airflow.exceptions import AirflowFailException
from requests.exceptions import RequestException

from patientdags.utils.constants import CCD_FILE_FORMAT, HL7_FILE_FORMAT
from patientdags.utils.error_codes import PatientDagsErrorCodes
from patientdags.utils.utils import publish_error_code


def _generate_access_token(client_id, client_secret):
    auth_url = "https://login.microsoftonline.com/c426113e-d3ce-4adb-bb91-c53b37c58f14/oauth2/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "resource": "https://hecfhir.azurehealthcareapis.com",
    }
    response = requests.request("GET", auth_url, headers=headers, data=payload)
    return f'Bearer {response.json()["access_token"]}'


def convert_hl7_to_fhir(data, file_format, file_type, client_id, client_secret):
    try:
        url = "https://hecfhir.azurehealthcareapis.com/$convert-data"
        access_token = _generate_access_token(client_id, client_secret)
        headers = {"Authorization": access_token, "Content-Type": "application/json"}
        parameters = [
            {"name": "inputData", "valueString": data},
        ]
        parameters.extend(_generate_params(file_format, file_type))
        payload = json.dumps(
            {
                "resourceType": "Parameters",
                "parameter": parameters,
            }
        )
        response = requests.request("POST", url, headers=headers, data=payload)
        if response is not None and response.status_code == 200:
            return response.text
        else:
            raise RequestException(response=response)
    except RequestException as e:
        logging.error(f"status code: {e.response.status_code}, status: {e.response.reason}")
        publish_error_code(PatientDagsErrorCodes.HL7_TO_FHIR_CONVERSION_ERROR.value)
        raise AirflowFailException(e)


def convert_ccd_to_fhir(data, file_format, client_id, client_secret):
    try:
        url = "https://hecfhir.azurehealthcareapis.com/$convert-data"
        access_token = _generate_access_token(client_id, client_secret)
        headers = {"Authorization": access_token, "Content-Type": "application/json"}
        parameters = [
            {"name": "inputData", "valueString": data},
        ]
        parameters.extend(_generate_params(file_format))
        payload = json.dumps(
            {
                "resourceType": "Parameters",
                "parameter": parameters,
            }
        )
        response = requests.request("POST", url, headers=headers, data=payload)
        if response is not None and response.status_code == 200:
            return response.text
        else:
            raise RequestException(response=response)
    except RequestException as e:
        logging.error(f"status code: {e.response.status_code}, status: {e.response.reason}")
        publish_error_code(PatientDagsErrorCodes.CCD_TO_FHIR_CONVERSION_ERROR.value)
        raise AirflowFailException(e)


def _generate_params(file_format, file_type=None):
    params = []
    if file_format == CCD_FILE_FORMAT:
        params.extend(
            [
                {"name": "inputDataType", "valueString": "Ccda"},
                {
                    "name": "templateCollectionReference",
                    "valueString": "microsofthealth/ccdatemplates:default",
                },
                {"name": "rootTemplate", "valueString": "CCD"},
            ]
        )
    elif file_format == HL7_FILE_FORMAT:
        params.extend(
            [
                {"name": "inputDataType", "valueString": "Hl7v2"},
                {
                    "name": "templateCollectionReference",
                    "valueString": "microsofthealth/hl7v2templates:default",
                },
                {"name": "rootTemplate", "valueString": file_type},
            ]
        )
    return params
