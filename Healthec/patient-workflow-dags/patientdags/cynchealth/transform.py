import json

import usaddress
from fhirtransformer.transformer import FHIRTransformer

from patientdags.cynchealth.constants import (  # CVX_CODE_SYSTEM,
    CPT_CODE_SYSTEM,
    ICT_X_CM_CODE_SYSTEM,
    LONIC_CODE_SYSTEM,
    TYPE_OTHER_CODE,
    TYPE_OTHER_DISPLAY,
    TYPE_PROV_CODE,
    TYPE_PROV_DISPLAY,
)
from patientdags.cynchealth.utils import (  # parse_marital_status,
    parse_date,
    parse_date_time,
    parse_encounter_class,
    parse_status,
    update_gender,
    update_observation_result,
)
from patientdags.utils.enum import ResourceType

RMSC_SYSTEM = "https://www.rockymtnseniorcare.com/"


def parse_address(value) -> dict:
    address_line_1 = ""
    address_line_2 = ""
    city = ""
    state = ""
    zip = ""
    address = usaddress.parse(value)
    addr_num = ""
    addr_street = ""
    addr_occup = ""
    for entry in address:
        if entry[1] == "AddressNumber":
            addr_num = entry[0]
        elif entry[1] == "StreetName":
            addr_street = " ".join([entry[0], addr_street]) if addr_street else entry[0]
        elif entry[1] == "StreetNamePostType":
            addr_street = " ".join([addr_street, entry[0]]) if addr_street else entry[0]
        elif entry[1] == "PlaceName":
            city = entry[0]
        elif entry[1] == "StateName":
            state = entry[0]
        elif entry[1] == "ZipCode":
            zip = entry[0]
        elif entry[1] == "OccupancyType":
            addr_occup = " ".join([entry[0], addr_occup]) if addr_occup else entry[0]
        elif entry[1] == "OccupancyIdentifier":
            addr_occup = " ".join([addr_occup, entry[0]]) if addr_occup else entry[0]
    if addr_street:
        address_line_1 = " ".join([addr_num, addr_street]) if addr_num else addr_street
    elif addr_occup:
        address_line_1 = " ".join([addr_num, addr_occup]) if addr_num else addr_occup
    if addr_street and addr_occup:
        address_line_2 = addr_occup
    return {
        "address_line_1": address_line_1,
        "address_line_2": address_line_2,
        "city": city,
        "state": state,
        "zip": zip,
    }


def transform_organization(row: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["source_id"] = row.get("Identifier", "")
    data_dict["source_system"] = RMSC_SYSTEM
    data_dict["active"] = parse_status(row.get("Active", ""))
    # organization type
    if row.get("Type", "") == "Medical Practice":
        data_dict["type_code"] = TYPE_PROV_CODE
        data_dict["type_display"] = TYPE_PROV_DISPLAY
    else:
        data_dict["type_code"] = TYPE_OTHER_CODE
        data_dict["type_display"] = TYPE_OTHER_DISPLAY
    data_dict["name"] = row.get("Name", "")
    data_dict["phone_work"] = row.get("Telecom", "")
    address = parse_address(row.get("Address", ""))
    data_dict["street_address_1"] = address.get("address_line_1", "")
    data_dict["street_address_2"] = address.get("address_line_2", "")
    data_dict["city"] = address.get("city", "")
    data_dict["state"] = address.get("state", "")
    data_dict["zip"] = address.get("zip", "")
    contact_name = row.get("Contact", "").split()
    data_dict["contact_firstname"] = contact_name[0] if len(contact_name) > 0 else ""
    data_dict["contact_lastname"] = contact_name[-1] if len(contact_name) > 1 else ""
    if len(contact_name) > 2:
        data_dict["contact_middlename"] = " ".join(contact_name[1:-1])
    resource_dict = json.loads(transformer.render_resource(ResourceType.Organization.value, data_dict))
    return resource_dict


def tranform_practitioner(row: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["source_id"] = row.get("Identifier", "")
    data_dict["source_system"] = RMSC_SYSTEM
    data_dict["active"] = parse_status(row.get("Active", ""))
    pract_name = row.get("Name", "").split()
    data_dict["firstname"] = pract_name[0] if len(pract_name) > 0 else ""
    data_dict["lastname"] = pract_name[-1] if len(pract_name) > 1 else ""
    if len(pract_name) > 2:
        data_dict["middleinitials"] = " ".join(pract_name[1:-1])
    data_dict["phone_work"] = row.get("Telecom", "")
    address = parse_address(row.get("Address", ""))
    data_dict["street_address_1"] = address.get("address_line_1", "")
    data_dict["street_address_2"] = address.get("address_line_2", "")
    data_dict["city"] = address.get("city", "")
    data_dict["state"] = address.get("state", "")
    data_dict["zip"] = address.get("zip", "")
    data_dict["gender"] = update_gender(row.get("Gender", ""))
    data_dict["dob"] = parse_date(row.get("BirthDate", ""))
    if row.get("Qualification", "") == "NPI":
        data_dict["npi"] = row.get("QualitificationIdentifier", "")
    resource_dict = json.loads(transformer.render_resource(ResourceType.Practitioner.value, data_dict))
    return resource_dict


def transform_patient(row: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["mrn"] = row.get("MRN", "")
    data_dict["mrn_system"] = RMSC_SYSTEM
    data_dict["assigner_organization_id"] = row.get("OrganizationRef", "")
    data_dict["organization_id"] = row.get("OrganizationRef", "")
    data_dict["active"] = parse_status(row.get("Active", ""))
    data_dict["lastname"] = row.get("LastName", "")
    data_dict["firstname"] = row.get("FirstName", "")
    data_dict["dob"] = parse_date(row.get("BirthDate", ""))
    data_dict["street_address_1"] = row.get("AddressStreet", "")
    data_dict["city"] = row.get("AddressCity", "")
    data_dict["country"] = row.get("AddressCountry", "")
    data_dict["state"] = row.get("AddressState", "")
    data_dict["zip"] = row.get("AddressZipcode", "")
    if row.get("PhoneType") == "Home":
        data_dict["phone_home"] = row.get("PhoneNumber", "")
    if row.get("PhoneType") == "Work":
        data_dict["phone_work"] = row.get("PhoneNumber", "")
    if row.get("PhoneType") == "Mobile":
        data_dict["phone_mobile"] = row.get("PhoneNumber", "")
    # apply `unknown` race and ethnicity
    # data_dict["race_code"] = "unknown"
    # data_dict["race_display"] = "Unknown"
    # data_dict["race"] = "Unknown"
    # data_dict["ethnicity_code"] = "unknown"
    # data_dict["ethnicity_display"] = "Unknown"
    # data_dict["ethnicity"] = "Unknown"
    # data_dict["race_code"] = row.get("Race", "")
    # data_dict["ethnicity_code"] = row.get("Ethnicity", "")
    # get_marital_status = parse_marital_status(row.get("MaritalStatus", ""))
    # data_dict["marital_status_display"] = get_marital_status.get("marital_status_display", "")
    # data_dict["marital_status_system"] = get_marital_status.get("marital_status_system", "")
    # data_dict["marital_status_code"] = get_marital_status.get("marital_status_code", "")
    # data_dict["marital_status_text"] = row.get("patient_marital_status_display", "")
    data_dict["gender"] = update_gender(row.get("Gender", ""))
    data_dict["preferred_language_text"] = row.get("PrimaryLanguage", "")
    resource_dict = json.loads(transformer.render_resource(ResourceType.Patient.value, data_dict))
    # TODO: This will be removed once race and ethnicity template issue is fixed
    # Added `unknown` race and ethnicity
    resource_dict["extension"] = [
        {
            "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
            "extension": [
                {
                    "url": "ombCategory",
                    "valueCoding": {
                        "code": "UNK",
                        "display": "unknown",
                        "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
                    },
                },
                {
                    "url": "text",
                    "valueString": "unknown",
                },
            ],
        },
        {
            "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
            "extension": [
                {
                    "url": "ombCategory",
                    "valueCoding": {
                        "code": "UNK",
                        "display": "unknown",
                        "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
                    },
                },
                {
                    "url": "text",
                    "valueString": "unknown",
                },
            ],
        },
    ]
    return resource_dict


def transform_encounter(row: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["source_id"] = row.get("ID", "")
    data_dict["source_system"] = RMSC_SYSTEM
    data_dict["assigner_organization_id"] = row.get("OrganizationRef", "")
    data_dict["patient_id"] = row.get("PatientRef", "")
    data_dict["practitioner_id"] = row.get("PractitionerRef", "")
    data_dict["status"] = row.get("Status", "")
    encounter_class = parse_encounter_class(row.get("ACTCode", ""))
    data_dict["class_code"] = encounter_class.get("code", "")
    data_dict["class_dispay"] = encounter_class.get("display", "")
    data_dict["type_text"] = row.get("Code", "")
    data_dict["period_start_date"] = parse_date_time(row.get("StartDateTime", ""))
    data_dict["period_end_date"] = parse_date_time(row.get("EndDateTime", ""))
    data_dict["physical_type_code"] = row.get("Location", "")
    data_dict["physical_type_system"] = RMSC_SYSTEM
    resource_dict = json.loads(transformer.render_resource(ResourceType.Encounter.value, data_dict))
    return resource_dict


def transform_observation(row: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["encounter_id"] = row.get("EncounterRef", "")
    data_dict["patient_id"] = row.get("PatientRef", "")
    data_dict["loinc_code"] = row.get("Code", "")
    data_dict["loinc_code_system"] = LONIC_CODE_SYSTEM
    data_dict["quantity_value"] = row.get("value", "")
    data_dict["quantity_unit"] = row.get("unit", "")
    data_dict["status"] = update_observation_result(row.get("Status", ""))
    resource_dict = json.loads(transformer.render_resource(ResourceType.Observation.value, data_dict))
    return resource_dict


def transform_immunization(row: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["encounter_id"] = row.get("EncounterRef", "")
    data_dict["patient_id"] = row.get("PatientRef", "")
    data_dict["status"] = row.get("Status", "")
    data_dict["vaccine_code_text"] = row.get("CVX", "")
    data_dict["occurrence_date_time"] = (
        parse_date_time(row.get("OccuranceDateTime")) if row.get("OccuranceDateTime") else ""
    )
    resource_dict = json.loads(transformer.render_resource(ResourceType.Immunization.value, data_dict))
    return resource_dict


def transform_procedure(row: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["encounter_id"] = row.get("EncounterRef", "")
    data_dict["patient_id"] = row.get("PatientRef", "")
    data_dict["code"] = row.get("Code", "")
    data_dict["code_system"] = CPT_CODE_SYSTEM
    data_dict["status"] = row.get("Status", "")
    data_dict["performed_period_start"] = parse_date_time(row.get("StartDateTime")) if row.get("StartDateTime") else ""
    data_dict["performed_period_end"] = parse_date_time(row.get("EndDateTime")) if row.get("EndDateTime") else ""
    resource_dict = json.loads(transformer.render_resource(ResourceType.Procedure.value, data_dict))
    return resource_dict


def transform_condition(row: dict, transformer: FHIRTransformer) -> dict:
    data_dict = {}
    data_dict["encounter_id"] = row.get("EncounterRef", "")
    data_dict["patient_id"] = row.get("PatientRef", "")
    data_dict["clinical_status"] = row.get("ClinicalStatus", "")
    data_dict["verification_status"] = row.get("VerificationStatus", "")
    data_dict["code"] = row.get("Code", "")
    data_dict["code_system"] = ICT_X_CM_CODE_SYSTEM
    data_dict["onset_date_time"] = parse_date_time(row.get("onsetDateTime")) if row.get("onsetDateTime") else ""
    data_dict["abatement_date_time"] = (
        parse_date_time(row.get("abatementDateTime")) if row.get("abatementDateTime") else ""
    )
    data_dict["recorded_date_time"] = parse_date_time(row.get("RecordedDateTime")) if row.get("RecordedDateTime") else ""
    resource_dict = json.loads(transformer.render_resource(ResourceType.Condition.value, data_dict))
    return resource_dict
