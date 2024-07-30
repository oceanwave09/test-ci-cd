import sys
import json
import logging

from fhirclient.configuration import Configuration
from fhirclient.client import FHIRClient
from fhirclient.resources.organization import Organization

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

# create organization client
client = FHIRClient(client_config)
org_client = Organization(client)

# create organization resource
with open("data/organization.json", "rb") as f:
    data = json.load(f)
org_resource_create = org_client.create(json.dumps(data))
logging.info("Create organization successful, %s", org_resource_create)

# get organization resource
org_id = org_resource_create.get("id")
logging.info("Organization Id: %s", org_id)
org_resource_get = org_client.get(org_id)
logging.info("Get organization successful, %s", org_resource_get)

# update organization resource
with open("data/organization_contact.json", "rb") as f:
    contact_data = json.load(f)
org_resource = {**org_resource_get.get("attributes"), **contact_data}
logging.info("Organization resource for update: %s", org_resource)
org_resource_update = org_client.update(org_id, json.dumps(org_resource))
if org_resource_update:
    logging.info("Update organization successful")
else:
    logging.info("Update organization unsuccessful")

# match organization resource
org_identifier = org_resource.get("identifier")[0]
logging.info("Organization identifier for match: %s", org_identifier)
org_id_matches = org_client.match(json.dumps({"identifier": [org_identifier]}))
if org_id_matches and len(org_id_matches) == 0:
    logging.info("No matched organization")
else:
    logging.info("Matched organization, %s", org_id_matches)
