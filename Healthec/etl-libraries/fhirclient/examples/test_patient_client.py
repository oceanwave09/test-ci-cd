import sys
import json
import logging

from fhirclient.configuration import Configuration
from fhirclient.client import FHIRClient
from fhirclient.resources.patient import Patient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# client configuration to make request with provider service
service_host = "development.healthec.com"
tenant_subdomain = "cynchealth"
protocol = "https"
auth_config = {
    "auth_host": "development.healthec.com",
    "auth_subdomain": "keycloak",
    "auth_tenant_subdomain": "cynchealth",
}
client_config = Configuration(
    protocol=protocol,
    service_host=service_host, 
    tenant_subdomain=tenant_subdomain, 
    auth_config=auth_config,
)

# create patient client
client = FHIRClient(client_config)
pat_client = Patient(client)

# create patient resource
with open("data/patient.json", "rb") as f:
    data = json.load(f)
pat_resource_create = pat_client.create(json.dumps(data))
logging.info("Create patient successful, %s", pat_resource_create)

# get patient resource
pat_id = pat_resource_create.get("id")
logging.info("Patient Id: %s", pat_id)
pat_resource_get = pat_client.get(pat_id)
logging.info("Get patient successful, %s", pat_resource_get)

# update patient resource
with open("data/patient_contact.json", "rb") as f:
    contact_data = json.load(f)
pat_resource = {**pat_resource_get.get("attributes"), **contact_data}
logging.info("Patient resource for update: %s", pat_resource)
pat_resource_update = pat_client.update(pat_id, json.dumps(pat_resource))
if pat_resource_update:
    logging.info("Update patient successful")
else:
    logging.info("Update patient unsuccessful")

# TODO: patient match should work with all identifier, currently it works only with SSN
# match patient resource
pat_identifier = pat_resource.get("identifier")[0]
logging.info("Patient identifier for match: %s", pat_identifier)
pat_id_matches = pat_client.match(json.dumps({"identifier": [pat_identifier]}))
if pat_id_matches is None or len(pat_id_matches) == 0:
    logging.info("No matched patient")
else:
    logging.info("Matched patient, %s", pat_id_matches)
