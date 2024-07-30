import sys
import json
import logging

from fhirclient.configuration import Configuration
from fhirclient.client import FHIRClient
from fhirclient.resources.practitioner import Practitioner

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

# create practitioner client
client = FHIRClient(client_config)
pract_client = Practitioner(client)

# create practitioner resource
with open("data/practitioner.json", "rb") as f:
    data = json.load(f)
pract_resource_create = pract_client.create(json.dumps(data))
logging.info("Create practitioner successful, %s", pract_resource_create)

# get practitioner resource
pract_id = pract_resource_create.get("id")
logging.info("Practitioner Id: %s", pract_id)
pract_resource_get = pract_client.get(pract_id)
logging.info("Get practitioner successful, %s", pract_resource_get)

# update practitioner resource
with open("data/practitioner_qualification.json", "rb") as f:
    qualification_data = json.load(f)
pract_resource = {**pract_resource_get.get("attributes"), **qualification_data}
logging.info("Practitioner resource for update: %s", pract_resource)
pract_resource_update = pract_client.update(pract_id, json.dumps(pract_resource))
if pract_resource_update:
    logging.info("Update practitioner successful")
else:
    logging.info("Update practitioner unsuccessful")

# TODO: practitioner match should work with identifier, currently it works only with id
# # match practitioner resource
# pract_identifier = pract_resource.get("identifier")[0]
# logging.info("Practitioner identifier for match: %s", pract_identifier)
# pract_id_matches = pract_client.match(json.dumps({"identifier": [pract_identifier]}))
# if pract_id_matches is None or len(pract_id_matches) == 0:
#     logging.info("No matched practitioner")
# else:
#     logging.info("Matched practitioner, %s", pract_id_matches)
