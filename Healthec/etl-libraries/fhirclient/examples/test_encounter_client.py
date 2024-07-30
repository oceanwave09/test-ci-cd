import sys
import json
import logging

from fhirclient.configuration import Configuration
from fhirclient.client import FHIRClient
from fhirclient.resources.encounter import Encounter

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
    resolve_reference=True            # enabled resolving reference
)

# create encounter client
client = FHIRClient(client_config)
enc_client = Encounter(client)

# create encounter resource
with open("data/encounter.json", "rb") as f:
    data = json.load(f)
enc_resource_create = enc_client.create(json.dumps(data))
logging.info("Create encounter successful, %s", enc_resource_create)

# get encounter resource
enc_id = enc_resource_create.get("id")
logging.info("Encounter Id: %s", enc_id)
enc_resource_get = enc_client.get(enc_id)
logging.info("Get encounter successful, %s", enc_resource_get)

# update encounter resource
with open("data/encounter_status.json", "rb") as f:
    status_data = json.load(f)
enc_resource = {**enc_resource_get.get("attributes"), **status_data}
logging.info("Encounter resource for update: %s", enc_resource)
enc_resource_update = enc_client.update(enc_id, json.dumps(enc_resource))
if enc_resource_update:
    logging.info("Update encounter successful")
else:
    logging.info("Update encounter unsuccessful")

# NOTE: encounter does not have identifier
# match encounter resource
# enc_identifier = enc_resource.get("identifier")[0]
# logging.info("Encounter identifier for match: %s", enc_identifier)
# enc_id_matches = enc_client.match(json.dumps({"identifier": [enc_identifier]}))
# if enc_id_matches is None or len(enc_id_matches) == 0:
#     logging.info("No matched encounter")
# else:
#     logging.info("Matched encounter, %s", enc_id_matches)
