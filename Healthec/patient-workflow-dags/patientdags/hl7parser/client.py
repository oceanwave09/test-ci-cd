import logging
# import json
from fhirclient.resources.organization import Organization as OrganizationEntity
from patientdags.utils.api_client import (
    get_resource_entity,
    post_resource_entity,
    match_resource_entity,
)


def get_child_org_id(tenant: str, parent_org_id: str, child_org_name: str, child_org_resource: dict) -> str:
    try:
        get_parent_org_details = get_resource_entity(
            tenant=tenant, resource_id=parent_org_id, entity=OrganizationEntity
        )
        # if type(get_parent_org_details) == str:
        #     get_parent_org_details = json.loads(get_parent_org_details)
        if get_parent_org_details:
            get_parent_org_name = get_parent_org_details.get("name")
            if get_parent_org_name and child_org_name and (child_org_name == get_parent_org_name):
                return get_parent_org_details.get("id")
            else:
                print("NOT MATCHED")
                child_org_resource.pop("id")
                match_child_org = match_resource_entity(
                    tenant=tenant, resource=child_org_resource, entity=OrganizationEntity
                )
                if match_child_org:
                    print("MATCH CHILD ORG", match_child_org)
                    return match_child_org
                else:
                    return post_resource_entity(
                        tenant=tenant, resource=child_org_resource, entity=OrganizationEntity
                    )
    except Exception as e:
        logging.error(f"Get sending facility error --> {str(e)}")
