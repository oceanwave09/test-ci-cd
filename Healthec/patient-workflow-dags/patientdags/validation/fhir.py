# import json
# import re
# import logging
# import random
# import string
# from datetime import datetime, timezone
# from string import Template
# from typing import Tuple

# import dateparser
# import ijson
# from airflow.exceptions import AirflowFailException, AirflowSkipException
# from fhirclient.resources.allergyintolerance import AllergyIntolerance
# from fhirclient.resources.claim import Claim
# from fhirclient.resources.claimresponse import ClaimResponse
# from fhirclient.resources.condition import Condition
# from fhirclient.resources.coverage import Coverage
# from fhirclient.resources.diagnosticreport import DiagnosticReport
# from fhirclient.resources.encounter import Encounter
# from fhirclient.resources.insuranceplan import InsurancePlan
# from fhirclient.resources.location import Location
# from fhirclient.resources.medication import Medication
# from fhirclient.resources.medicationstatement import MedicationStatement
# from fhirclient.resources.observation import Observation
# from fhirclient.resources.organization import Organization
# from fhirclient.resources.patient import Patient
# from fhirclient.resources.practitioner import Practitioner
# from fhirclient.resources.practitionerrole import PractitionerRole
# from fhirclient.resources.procedure import Procedure
# from jsonmerge import Merger

# from patientdags.utils.api_client import (
#     post_core_entity,
#     post_organization_sub_entity,
#     post_patient_sub_entity,
#     post_practitioner_sub_entity,
# )
# from patientdags.utils.constants import PRACTITIONER_MERGER_SCHEMA
# from patientdags.utils.enum import ResourceType
# from patientdags.utils.error_codes import PatientDagsErrorCodes, publish_error_code


# def _get_human_fullname(resource: dict) -> str:
#     # fullname is derived from `<family> <given[0]>`
#     if len(resource.get("name", [])) == 0:
#         return ""
#     name = resource.get("name")[0]
#     last_name = name.get("family")
#     first_name = name.get("given")[0] if len(name.get("given", [])) > 0 else ""
#     return f"{last_name} {first_name}"


# # def _update_managing_organization_id(entry_data: dict, organization_ids_str: str, src_organization_id: str = None):
# #     organizatinon = entry_data.get("managingOrganization")
# #     if organizatinon is None:
# #         entry_data.update({"managingOrganization": {}})
# #         organizatinon = entry_data.get("managingOrganization")

# #     organization_ids = json.loads(organization_ids_str) if organization_ids_str else {}
# #     print(f"organization_ids: {organization_ids}")

# #     if src_organization_id:
# #         organizatinon_id = src_organization_id
# #     elif DEFAULT_FHIR_ORG_ID:
# #         organizatinon_id = DEFAULT_FHIR_ORG_ID
# #     elif organizatinon:
# #         organizatinon_id_exists = bool(organizatinon.get("id"))
# #         # TODO: Update to removeprefix after upgrading to python 3.9
# #         old_organizatinon_id = (
# #             organizatinon.get("id")
# #             if organizatinon_id_exists
# #             else organizatinon.get("reference").replace("Organization/", "")
# #         )
# #         organizatinon_id = organization_ids.get(old_organizatinon_id)
# #     elif len(organization_ids) > 0:
# #         organizatinon_id = [*organization_ids.values()][0]

# #     organizatinon.update(
# #         {
# #             "id": organizatinon_id,
# #             "reference": f"Organization/{organizatinon_id}",
# #             "type": "Organization",
# #         }
# #     )


# # def _update_organization_id(
# #     entry_data: dict,
# #     organization_ids: dict,
# #     organization_field: str,
# #     src_organization_id: str = None,
# #     is_required: bool = False,
# # ):
# #     organizatinon = entry_data.get(organization_field)
# #     if organizatinon is None:
# #         entry_data.update({organization_field: {}})
# #         organizatinon = entry_data.get(organization_field)

# #     organizatinon_id = ""
# #     if src_organization_id:
# #         organizatinon_id = src_organization_id
# #     elif organizatinon:
# #         organizatinon_id_exists = bool(organizatinon.get("id"))
# #         # TODO: Update to removeprefix after upgrading to python 3.9
# #         old_organization_id = (
# #             organizatinon.get("id")
# #             if organizatinon_id_exists
# #             else organizatinon.get("reference").replace("Organization/", "")
# #         )
# #         organizatinon_id = organization_ids.get(old_organization_id)
# #     elif is_required and len(organization_ids) > 0:
# #         organizatinon_id = [*organization_ids.values()][0]

# #     if organizatinon_id:
# #         organizatinon.update(
# #             {"id": organizatinon_id, "reference": f"Organization/{organizatinon_id}", "type": "Organization"}
# #         )


# # def _update_patient_id(entry_data: dict, patient_ids: dict, patient_field: str):
# #     patient_id = None
# #     patient = entry_data.get(patient_field)
# #     if patient:
# #         patient_id_exists = bool(patient.get("id"))
# #         # TODO: Update to removeprefix after upgrading to python 3.9
# #         old_patient_id = patient.get("id") if patient_id_exists else patient.get("reference").replace("Patient/", "")
# #         patient_id = patient_ids.get(old_patient_id)
# #     elif len(patient_ids) > 0:
# #         # TODO: Remove else part and fix condition to avoid automatic user assignment
# #         patient_id = [*patient_ids.values()][0]
# #         entry_data.update({patient_field: {}})
# #         patient = entry_data.get(patient_field)
# #     if patient_id:
# #         patient.update({"id": patient_id, "reference": f"Patient/{patient_id}", "type": "Patient"})


# def _update_location_id(entry_data: dict, location_ids: dict, src_location_id: str = None):
#     locations = entry_data.get("location")
#     if locations:
#         for location in locations:
#             print(f"location: {location}")
#             if src_location_id:
#                 location_id = src_location_id
#             else:
#                 location_id_exists = bool(location.get("location", {}).get("id"))
#                 old_location_id = (
#                     location.get("location", {}).get("id")
#                     if location_id_exists
#                     else location.get("location").get("reference").replace("Location/", "")
#                 )
#                 print(f"old_location_id: {old_location_id}")
#                 location_id = location_ids[old_location_id]
#             print(f"location_id: {location_id}")
#             if location_id:
#                 location.get("location").update(
#                     {
#                         "id": location_id,
#                         "reference": f"Location/{location_id}",
#                         "type": "Location",
#                     }
#                 )


# def _update_practitioner_id(entry_data: dict, practitioner_ids: dict, src_practitioner_id: str = None):
#     practitioners = entry_data.get("participant")
#     print(f"practitioners: {practitioners}")
#     if practitioners:
#         for practitioner in practitioners:
#             print(f"practitioner: {practitioner}")
#             if src_practitioner_id:
#                 practitioner_id = src_practitioner_id
#             else:
#                 practitioner_id_exists = bool(practitioner.get("individual", {}).get("id"))
#                 old_practitioner_id = (
#                     practitioner.get("individual", {}).get("id")
#                     if practitioner_id_exists
#                     else practitioner.get("individual").get("reference").replace("Practitioner/", "")
#                 )
#                 print(f"old_practitioner_id: {old_practitioner_id}")
#                 practitioner_id = practitioner_ids[old_practitioner_id]
#             print(f"practitioner_id: {practitioner_id}")
#             if practitioner_id:
#                 practitioner.get("individual").update(
#                     {
#                         "id": practitioner_id,
#                         "reference": f"Practitioner/{practitioner_id}",
#                         "type": "Practitioner",
#                     }
#                 )
#     print(f"updated practitioners: {entry_data.get('participant')}")


# def _update_service_provider(entry_data: dict, src_organization_id: str):
#     if src_organization_id:
#         entry_data.update(
#             {
#                 "serviceProvider": {
#                     "id": src_organization_id,
#                     "reference": f"Organization/{src_organization_id}",
#                     "type": "Organization",
#                 }
#             }
#         )


# def _update_reference_id(
#     resource: str,
#     entry_data: dict,
#     reference_ids: str,
#     field: str,
#     default_id: str = None,
#     is_required: bool = False,
# ):
#     resource_field = entry_data.get(field)
#     if resource_field is None:
#         entry_data.update({field: {}})
#         resource_field = entry_data.get(field)
#     reference_ids = json.loads(reference_ids) if not is_empty(reference_ids) else {}

#     reference_id = ""
#     if default_id:
#         reference_id = default_id
#     elif resource_field:
#         reference_id_exists = bool(resource_field.get("id"))
#         # TODO: Update to removeprefix after upgrading to python 3.9
#         old_reference_id = (
#             resource_field.get("id")
#             if reference_id_exists
#             else resource_field.get("reference").replace(f"{resource}/", "")
#         )
#         reference_id = reference_ids.get(old_reference_id)
#     elif is_required and len(reference_ids) > 0:
#         reference_id = [*reference_ids.values()][0]

#     if reference_id:
#         resource_field.update({"id": reference_id, "reference": f"resource/{reference_id}", "type": resource})


# def _temp_add_race_extension(entry_data: dict):
#     # extension_url = ("http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",)
#     # does_race_extension_exist = False
#     # for extension in entry_data.get("extension"):
#     #     if extension.get("extension") and extension.get("url") == extension_url:
#     #         does_race_extension_exist = True
#     #         break

#     # if not entry_data.get("extension"):
#     #     entry_data.update({"extension": []})
#     # if not does_race_extension_exist:
#     #     entry_data["extension"].append(
#     #         {
#     #             "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
#     #             "extension": [
#     #                 {
#     #                     "url": "ombCategory",
#     #                     "valueCoding": {"code": "2106-3", "display": "White"},
#     #                 },
#     #                 {"url": "text", "valueString": "White"},
#     #             ],
#     #         }
#     #     )
#     entry_data["extension"].append(
#         {
#             "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
#             "extension": [
#                 {
#                     "system": "http://terminology.hl7.org/CodeSystem/v3-Race",
#                     "url": "ombCategory",
#                     "valueCoding": {
#                         "code": "UNK",
#                         "display": "unknown",
#                         "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
#                     },
#                 },
#                 {
#                     "url": "text",
#                     "valueString": "unknown",
#                 },
#             ],
#         }
#     )


# def _temp_add_ethnicity_extension(entry_data: dict):
#     # extension_url = (
#     #     "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
#     # )
#     # does_ethnicity_extension_exist = False
#     # for extension in entry_data.get("extension"):
#     #     if extension.get("extension") and extension.get("url") == extension_url:
#     #         does_ethnicity_extension_exist = True
#     #         break

#     # if not entry_data.get("extension"):
#     #     entry_data.update({"extension": []})
#     # if not does_ethnicity_extension_exist:
#     #     entry_data["extension"].append(
#     #         {
#     #             "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
#     #             "extension": [
#     #                 {
#     #                     "url": "ombCategory",
#     #                     "valueCoding": {
#     #                         "code": "2186-5",
#     #                         "display": "Not Hispanic or Latino",
#     #                     },
#     #                 },
#     #                 {"url": "text", "valueString": "Not Hispanic or Latino"},
#     #             ],
#     #         }
#     #     )
#     entry_data["extension"].append(
#         {
#             "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
#             "extension": [
#                 {
#                     "system": "http://terminology.hl7.org/CodeSystem/v3-Ethnicity",
#                     "url": "ombCategory",
#                     "valueCoding": {
#                         "code": "UNK",
#                         "display": "unknown",
#                         "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
#                     },
#                 },
#                 {
#                     "url": "text",
#                     "valueString": "unknown",
#                 },
#             ],
#         }
#     )


# # def _add_identifier_use_value(entry_data: dict):
# #     identifiers = entry_data.get("identifier", [])
# #     for identifier in identifiers:
# #         if not identifier.get("use"):
# #             identifier.update({"use": "temp"})


# def _temp_remove_identifier(entry_data: dict):
#     # TODO: Temporarily remove identifier from Observation resources
#     entry_data.update({"identifier": []})


# def _temp_add_npi_identifier(entry_data):
#     npi_template = """
#         {
#             "identifier": [
#                 {
#                     "type": {
#                         "coding": [
#                             {
#                                 "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
#                                 "code": "NPI"
#                             }
#                         ]
#                     },
#                     "system": "http://hl7.org/fhir/sid/us-npi",
#                     "value": "$random_npi"
#                 }
#             ]
#         }
#     """
#     npi_identifier = Template(npi_template).substitute(random_npi=_generate_random_number(10))
#     entry_data.update(json.loads(npi_identifier))


# def _temp_add_ssn_identifier(entry_data):
#     ssn_template = """
#         {
#             "identifier": [
#                 {
#                     "type": {
#                         "coding": [
#                             {
#                                 "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
#                                 "code": "SS"
#                             }
#                         ]
#                     },
#                     "system": "http://hl7.org/fhir/sid/us-ssn",
#                     "value": "$random_ssn"
#                 }
#             ]
#         }
#     """
#     ssn_identifier = Template(ssn_template).substitute(random_ssn=_generate_random_number(9))
#     entry_data.update(json.loads(ssn_identifier))


# def _temp_add_tax_identifier(entry_data):
#     tin_template = """
#         {
#             "identifier": [
#                 {
#                     "type": {
#                         "coding": [
#                             {
#                                 "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
#                                 "code": "TAX"
#                             }
#                         ]
#                     },
#                     "system": "urn:oid:2.16.840.1.113883.4.2",
#                     "value": "$random_tin"
#                 }
#             ]
#         }
#     """
#     tin_identifier = Template(tin_template).substitute(random_tin=_generate_random_number(9))
#     print(tin_identifier)
#     print(json.loads(tin_identifier))
#     entry_data.update(json.loads(tin_identifier))


# def _temp_validate_tax_identifier(entry_data: dict):
#     identifiers = entry_data.get("identifier", [])
#     for identifier in identifiers:
#         identifier_type = identifier.get("type")
#         if identifier_type and identifier_type.get("coding") and identifier_type.get("coding")[0].get("code") == "TAX":
#             # TODO: Fix tax identifier system value in platform service
#             if identifier.get("system") != "urn:oid:2.16.840.1.113883.4.2":
#                 identifier["system"] = "urn:oid:2.16.840.1.113883.4.2"
#             break


# def _temp_validate_identifiers(entry_data: dict, src_organization_id: str = None):
#     identifiers = entry_data.get("identifier", [])
#     if len(identifiers) == 0:
#         # TODO: Adding identifier with random value incase of no identifier
#         if entry_data.get("resourceType") == ResourceType.Practitioner.value:
#             _temp_add_npi_identifier(entry_data)
#         elif entry_data.get("resourceType") == ResourceType.Patient.value:
#             _temp_add_ssn_identifier(entry_data)
#         elif entry_data.get("resourceType") == ResourceType.Organization.value:
#             _temp_add_tax_identifier(entry_data)
#     else:
#         for identifier in identifiers:
#             if not identifier.get("system"):
#                 # TODO: Adding default system if it is not present in identifier
#                 identifier["system"] = "http://healthec.com/default/system"
#             if src_organization_id and (
#                 len(identifier.get("type", {}).get("coding", [])) == 0
#                 or identifier.get("type", {}).get("coding", [])[0].get("code") == "MR"
#             ):
#                 identifier.update(
#                     {
#                         "assigner": {
#                             "id": src_organization_id,
#                             "reference": f"Organization/{src_organization_id}",
#                             "type": "Organization",
#                         }
#                     }
#                 )


# def _temp_validate_procedure_encounter(entry_data: dict, encounter_ids: dict):
#     if not entry_data.get("encounter"):
#         if len(encounter_ids) > 0:
#             encounter_id = [*encounter_ids.values()][0]
#     else:
#         encounter_id_exists = bool(entry_data["encounter"].get("id"))
#         # TODO: Update to removeprefix after updating to python 3.9
#         old_encounter_id = (
#             entry_data["encounter"].get("id")
#             if encounter_id_exists
#             else entry_data["encounter"].get("reference").replace("Encounter/", "")
#         )
#         encounter_id = encounter_ids[old_encounter_id]

#     entry_data.update(
#         {
#             "encounter": {
#                 "id": encounter_id,
#                 "reference": f"Encounter/{encounter_id}",
#                 "type": "Encounter",
#             }
#         }
#     )


# def _validate_organization_type(entity_data: dict):
#     if entity_data.get("type") is None:
#         # TODO: Adding `other` as organization type if it does not exists
#         entity_data.update(
#             {
#                 "type": [
#                     {
#                         "coding": [
#                             {
#                                 "code": "other",
#                                 "display": "Other",
#                                 "definition": "Other type of organization not already specified.",
#                                 "system": "http://terminology.hl7.org/CodeSystem/organization-type",
#                             }
#                         ]
#                     }
#                 ]
#             }
#         )


# # def _validate_organization_name(entity_data: dict):
# #     if entity_data.get("name") is None:
# #         # TODO: Adding `HEALTHEC` as organization name if it does not exists
# #         entity_data.update({"name": "HEALTHEC"})


# # def _check_extension_coding_system(entry_data: dict):
# #     if entry_data.get("extension"):
# #         for extension in entry_data.get("extension"):
# #             if extension.get("extension"):
# #                 for extension_details in extension.get("extension"):
# #                     value_coding = extension_details.get("valueCoding")
# #                     if value_coding and value_coding.get("system") is None:
# #                         logging.info(value_coding)
# #                         value_coding.update({"system": "http://acme.com/config/fhir/codesystems/internal"})


# def _check_human_name(entry_data: dict):
#     comma = ","
#     if len(entry_data.get("name", [])) > 0:
#         if len(entry_data.get("name")[0].get("family", "")) == 0:
#             # update default last name
#             entry_data["name"][0]["family"] = "LName"
#         if len(entry_data.get("name")[0].get("given", [])) == 0:
#             # if last name has comma, split into last and first name
#             if comma in str(entry_data.get("name")[0].get("family", "")):
#                 family_split = str(entry_data.get("name")[0].get("family", "")).split(comma)
#                 entry_data["name"][0]["family"] = family_split[0]
#                 entry_data["name"][0]["given"] = [family_split[1]]
#             else:
#                 # update default first name
#                 entry_data["name"][0]["given"] = ["FName"]
#     else:
#         # update with defaut values for first and last name
#         entry_data["name"] = [{"family": "LName", "given": ["FName"]}]


# def _check_telecom_email(entry_data: dict):
#     if not entry_data.get("telecom"):
#         entry_data.update({"telecom": []})
#     email_exist = False
#     for telecom_field in entry_data.get("telecom"):
#         if telecom_field.get("system") == "email":
#             email_exist = True

#     if not email_exist:
#         default_email = f"default_{_generate_random_string()}@mail.com"
#         entry_data["telecom"].append({"system": "email", "value": default_email, "use": "work"})


# def _check_status(entry_data: dict):
#     if not entry_data.get("status") or str(entry_data.get("status")).lower() == "unknown":
#         entry_data.update({"status": "finished"})


# def _check_procedure_event_status(entry_data: dict):
#     statuses_to_change = {"active": "in-progress"}
#     if entry_data.get("status") in statuses_to_change.keys():
#         entry_data.update({"status": statuses_to_change[entry_data["status"]]})


# def _check_period(entry_data: dict):
#     if entry_data.get("period"):
#         period_start, period_end = _format_period(entry_data.get("period"))
#         entry_data.update({"period": {"start": period_start, "end": period_end}})


# # def _check_observation_effective(entry_data: dict):
# #     if entry_data.get("effectiveDateTime"):
# #         effective_datetime = _format_datetime(entry_data.get("effectiveDateTime"))
# #         if not effective_datetime:
# #             effective_datetime = datetime.utcnow().isoformat()
# #         entry_data.update({"effectiveDateTime": effective_datetime})
# #     if entry_data.get("effectivePeriod"):
# #         effective_start, effective_end = _format_period(entry_data.get("effectivePeriod"))
# #         entry_data.update({"effectivePeriod": {"start": effective_start, "end": effective_end}})


# # def _check_condition_onset(entry_data: dict):
# #     if entry_data.get("onsetDateTime"):
# #         onset_datetime = _format_datetime(entry_data.get("onsetDateTime"))
# #         if not onset_datetime:
# #             onset_datetime = datetime.utcnow().isoformat()
# #         entry_data.update({"onsetDateTime": onset_datetime})
# #     if entry_data.get("onsetPeriod"):
# #         onset_start, onset_end = _format_period(entry_data.get("onsetPeriod"))
# #         entry_data.update({"onsetPeriod": {"start": onset_start, "end": onset_end}})


# # def _check_condition_abatement(entry_data: dict):
# #     if entry_data.get("abatementDateTime"):
# #         abatement_datetime = _format_datetime(entry_data.get("abatementDateTime"))
# #         if not abatement_datetime:
# #             abatement_datetime = datetime.utcnow().isoformat()
# #         entry_data.update({"abatementDateTime": abatement_datetime})
# #     if entry_data.get("abatementPeriod"):
# #         abatement_start, abatement_end = _format_period(entry_data.get("abatementPeriod"))
# #         entry_data.update({"abatementPeriod": {"start": abatement_start, "end": abatement_end}})


# # def _check_procedure_performed(entry_data: dict):
# #     if entry_data.get("performedDateTime"):
# #         performed_datetime = _format_datetime(entry_data.get("performedDateTime"))
# #         if not performed_datetime:
# #             performed_datetime = datetime.utcnow().isoformat()
# #         entry_data.update({"performedDateTime": performed_datetime})
# #     if entry_data.get("performedPeriod"):
# #         performed_start, performed_end = _format_period(entry_data.get("performedPeriod"))
# #         entry_data.update({"performedPeriod": {"start": performed_start, "end": performed_end}})


# def _check_date_time(field: str, entry_data: dict):
#     datetime_field = f"{field}DateTime"
#     if entry_data.get(datetime_field):
#         datetime_value = _format_datetime(entry_data.get(datetime_field))
#         if not datetime_value:
#             datetime_value = datetime.utcnow().isoformat()
#         entry_data.update({datetime_field: datetime_value})
#     period_field = f"{field}Period"
#     if entry_data.get(period_field):
#         period_start, period_end = _format_period(entry_data.get(period_field))
#         entry_data.update({period_field: {"start": period_start, "end": period_end}})


# # def _check_location_address(entry_data: dict):
# #     if entry_data.get("address") is None:
# #         # TODO: Adding default address if it does not exists
# #         entry_data.update(
# #             {
# #                 "address": {
# #                     "line": ["343 Thornall Street", "Suite #630"],
# #                     "city": "Edison",
# #                     "state": "NJ",
# #                     "postalCode": "08837",
# #                 }
# #             }
# #         )


# # functions to validate fhir resources


# def _validate_entries(entries_data: dict):
#     valid_entries = []
#     for entry_data in entries_data:
#         entry_resource = entry_data.get("resource")
#         if entry_resource.get("id"):
#             entry_data.update({"id": _remove_old_id(entry_resource)})
#             _remove_resource_type(entry_resource)
#             valid_entries.append(entry_data)
#     return valid_entries


# def validate_organizations(organization_entries: str) -> str:
#     organization_entries = json.loads(organization_entries)
#     for entry in organization_entries:
#         entry_data = entry.get("resource")
#         _validate_organization_name(entry_data)
#         _validate_organization_type(entry_data)
#         _temp_validate_tax_identifier(entry_data)
#         _temp_validate_identifiers(entry_data)

#     organization_entries = _validate_entries(organization_entries)

#     return json.dumps(organization_entries)


# def validate_locations(location_entries: str, organization_ids: str, src_organization_id: str) -> str:
#     location_entries = json.loads(location_entries)
#     for entry in location_entries:
#         entry_data = entry.get("resource")
#         _update_reference_id(
#             "Organization", entry_data, organization_ids, "managingOrganization", src_organization_id, True
#         )
#         # _check_location_address(entry_data)

#     location_entries = _validate_entries(location_entries)

#     return json.dumps(location_entries)


# def validate_practitioners(practitioner_entries: str) -> str:
#     practitioner_entries = json.loads(practitioner_entries)
#     for entry in practitioner_entries:
#         entry_data = entry.get("resource")
#         _temp_validate_identifiers(entry_data)
#         _check_human_name(entry_data)
#         _check_telecom_email(entry_data)

#     practitioner_entries = _validate_entries(practitioner_entries)

#     return json.dumps(practitioner_entries)


# def validate_insurance_plans(
#     insurance_plan_entries: str, organization_ids_str: str, src_organization_id: str = None
# ) -> str:
#     insurance_plan_entries = json.loads(insurance_plan_entries)
#     organization_ids = json.loads(organization_ids_str) if organization_ids_str else {}

#     for entry in insurance_plan_entries:
#         entry_data = entry.get("resource")
#         _temp_validate_identifiers(entry_data)
#         _update_reference_id("Organization", entry_data, organization_ids, "ownedBy", src_organization_id)

#     insurance_plan_entries = _validate_entries(insurance_plan_entries)

#     return json.dumps(insurance_plan_entries)


# def validate_patients(patient_entries: str, organization_ids: str, src_organization_id: str) -> str:
#     patient_entries = json.loads(patient_entries)
#     for entry in patient_entries:
#         entry_data = entry.get("resource")
#         _update_reference_id(
#             "Organization", entry_data, organization_ids, "managingOrganization", src_organization_id, True
#         )
#         _temp_validate_identifiers(entry_data, src_organization_id)
#         entry_data.update({"extension": []})
#         _temp_add_race_extension(entry_data)
#         _temp_add_ethnicity_extension(entry_data)
#         # _check_extension_coding_system(entry_data)

#     patient_entries = _validate_entries(patient_entries)

#     return json.dumps(patient_entries)


# def validate_conditions(condition_entries: str, patient_ids: str) -> str:
#     condition_entries = json.loads(condition_entries)
#     patient_ids = json.loads(patient_ids)
#     for entry in condition_entries:
#         entry_data = entry.get("resource")
#         # _add_identifier_use_value(entry_data)
#         _check_date_time("onset", entry_data)
#         _check_date_time("abatement", entry_data)
#         _update_reference_id("Patient", entry_data, patient_ids, "subject", None, True)

#     condition_entries = _validate_entries(condition_entries)

#     return json.dumps(condition_entries)


# def validate_coverages(coverage_entries: str, patient_ids: str, organization_ids: str, insurance_plan_ids: str) -> str:
#     coverage_entries = json.loads(coverage_entries)
#     patient_ids = json.loads(patient_ids) if patient_ids else {}
#     organization_ids = json.loads(organization_ids) if organization_ids else {}
#     insurance_plan_ids = json.loads(insurance_plan_ids) if insurance_plan_ids else {}
#     for entry in coverage_entries:
#         entry_data = entry.get("resource")
#         _temp_validate_identifiers(entry_data)
#         _update_reference_id("Patient", entry_data, patient_ids, "beneficiary", None, True)
#         _update_reference_id("Patient", entry_data, patient_ids, "subscriber", None)
#         # _update_organization_id(entry_data, organization_ids, "insurer")
#         # _update_insurance_plan_id(entry_data, insurance_plan_ids, "insurancePlan")

#     coverage_entries = _validate_entries(coverage_entries)

#     return json.dumps(coverage_entries)


# def validate_encounters(
#     encounter_entries: str,
#     patient_ids: str,
#     practitioner_ids: str,
#     location_ids: str,
#     src_practitioner_id: str = None,
#     src_location_id: str = None,
#     src_organization_id: str = None,
# ) -> str:
#     encounter_entries = json.loads(encounter_entries)
#     patient_ids = json.loads(patient_ids)
#     practitioner_ids = json.loads(practitioner_ids)
#     location_ids = json.loads(location_ids)
#     for entry in encounter_entries:
#         entry_data = entry.get("resource")
#         # _add_identifier_use_value(entry_data)
#         _check_period(entry_data)
#         _check_status(entry_data)
#         _update_reference_id("Patient", entry_data, patient_ids, "subject", None, True)
#         _update_practitioner_id(entry_data, practitioner_ids, src_practitioner_id)
#         _update_location_id(entry_data, location_ids, src_location_id)
#         _update_service_provider(entry_data, src_organization_id)

#     encounter_entries = _validate_entries(encounter_entries)

#     return json.dumps(encounter_entries)


# def validate_allergies(allergy_entries: str, patient_ids: str) -> str:
#     allergy_entries = json.loads(allergy_entries)
#     for entry in allergy_entries:
#         entry_data = entry.get("resource")
#         # _add_identifier_use_value(entry_data)
#         _update_reference_id("Patient", entry_data, patient_ids, "patient", None, True)

#     allergy_entries = _validate_entries(allergy_entries)

#     return json.dumps(allergy_entries)


# def validate_diagnostic_reports(diagnostic_report_entries: str, patient_ids: str) -> str:
#     diagnostic_report_entries = json.loads(diagnostic_report_entries)
#     patient_ids = json.loads(patient_ids)
#     for entry in diagnostic_report_entries:
#         entry_data = entry.get("resource")
#         # _add_identifier_use_value(entry_data)
#         _update_reference_id("Patient", entry_data, patient_ids, "subject", None, True)

#     diagnostic_report_entries = _validate_entries(diagnostic_report_entries)

#     return json.dumps(diagnostic_report_entries)


# def validate_observations(observation_entries: str, patient_ids: str) -> str:
#     observation_entries = json.loads(observation_entries)
#     for entry in observation_entries:
#         entry_data = entry.get("resource")
#         # _add_identifier_use_value(entry_data)
#         _temp_remove_identifier(entry_data)
#         _check_date_time("effective", entry_data)
#         _update_reference_id("Patient", entry_data, patient_ids, "subject", None, True)

#     observation_entries = _validate_entries(observation_entries)

#     return json.dumps(observation_entries)


# def validate_medication_statements(medication_statement_entries: str, patient_ids: str) -> str:
#     medication_statement_entries = json.loads(medication_statement_entries)
#     patient_ids = json.loads(patient_ids)
#     for entry in medication_statement_entries:
#         entry_data = entry.get("resource")
#         # _add_identifier_use_value(entry_data)
#         _update_reference_id("Patient", entry_data, patient_ids, "subject", None, True)

#     medication_statement_entries = _validate_entries(medication_statement_entries)

#     return json.dumps(medication_statement_entries)


# def validate_procedures(procedure_entries: str, patient_ids: str, encounter_ids: str) -> str:
#     procedure_entries = json.loads(procedure_entries)
#     encounter_ids = json.loads(encounter_ids)
#     for entry in procedure_entries:
#         entry_data = entry.get("resource")
#         # _add_identifier_use_value(entry_data)
#         _update_reference_id("Patient", entry_data, patient_ids, "subject", None, True)
#         _check_procedure_event_status(entry_data)
#         _check_date_time("performed", entry_data)
#         _temp_validate_procedure_encounter(entry_data, encounter_ids)

#     procedure_entries = _validate_entries(procedure_entries)

#     return json.dumps(procedure_entries)


# def validate_claims(claim_entries: str, patient_ids: str) -> str:
#     claim_entries = json.loads(claim_entries)
#     for entry in claim_entries:
#         entry_data = entry.get("resource")
#         _temp_validate_identifiers(entry_data)
#         _update_reference_id("Patient", entry_data, patient_ids, "patient", None, True)

#     claim_entries = _validate_entries(claim_entries)

#     return json.dumps(claim_entries)


# def validate_claim_responses(claim_response_entries: str, patient_ids: str) -> str:
#     claim_response_entries = json.loads(claim_response_entries)
#     for entry in claim_response_entries:
#         entry_data = entry.get("resource")
#         _temp_validate_identifiers(entry_data)
#         _update_reference_id("Patient", entry_data, patient_ids, "patient", None, True)

#     claim_response_entries = _validate_entries(claim_response_entries)

#     return json.dumps(claim_response_entries)


# # validate fhirbundle
# def validate_fhirbundle(
#     fhir_bundle: str,
#     src_organization_id: str = None,
#     src_location_id: str = None,
#     src_practitioner_id: str = None,
# ) -> str:
#     fhir_bundle = json.loads(fhir_bundle)
#     fhir_entries = fhir_bundle.get("entry", [])
#     valid_fhir_entries = []
#     practitioner_entries = []
#     for entry in fhir_entries:
#         resource_type = entry.get("resource", {}).get("resourceType")
#         # organization resources skipped if source organization is provided
#         if resource_type == ResourceType.Organization.value and src_organization_id:
#             continue
#         # location resources skipped if source location is provided
#         if resource_type == ResourceType.Location.value and src_location_id:
#             continue
#         # practitioner resources skipped if source practitioner is provided
#         if resource_type == ResourceType.Practitioner.value:
#             if src_practitioner_id:
#                 continue
#             else:
#                 practitioner_entries.append(entry)
#                 continue
#         # document reference resources skipped
#         if resource_type == ResourceType.DocumentReference.value:
#             continue
#         valid_fhir_entries.append(entry)

#     valid_practitioner_entries, practitioner_merge_ids = merge_practitioners(practitioner_entries)
#     valid_fhir_entries.extend(valid_practitioner_entries)
#     fhir_bundle.update({"entry": valid_fhir_entries})
#     fhir_bundle_str = json.dumps(fhir_bundle)

#     # update practitioner's merge ids with base id
#     for merge_id, base_id in practitioner_merge_ids.items():
#         fhir_bundle_str = fhir_bundle_str.replace(merge_id, base_id)

#     return fhir_bundle_str


# def merge_practitioners(entries: list) -> Tuple[list, dict]:
#     final_entries = []
#     groups = {}
#     for entry in entries:
#         group_key = _get_human_fullname(entry.get("resource", {}))
#         if not group_key:
#             final_entries.append(entry)
#             continue
#         if groups.get(group_key):
#             groups[group_key].append(entry)
#         else:
#             groups[group_key] = [entry]
#     logging.info(f"groups: {groups}")

#     merger = Merger(PRACTITIONER_MERGER_SCHEMA)
#     merge_ids = {}
#     for group_key, group_entries in groups.items():
#         if len(group_entries) <= 1:
#             final_entries.extend(group_entries)
#             continue
#         # assign first entry as base
#         base_entry = group_entries[0]
#         base_resource = base_entry.get("resource")
#         base_id = base_resource.get("id")

#         for i in range(1, len(group_entries)):
#             head_reasource = group_entries[i].get("resource")
#             base_resource = merger.merge(base_resource, head_reasource)
#             merge_ids.update({head_reasource.get("id"): base_id})

#         base_entry.update({"resource": base_resource})
#         logging.info(f"merged base entry: {base_entry}")
#         final_entries.append(base_entry)

#     logging.info(f"final_entries: {final_entries}")
#     logging.info(f"merge_ids: {merge_ids}")
#     return final_entries, merge_ids


# # ==========================================================================================


# def _is_empty(value: str):
#     if not value or value == "None":
#         return True
#     return False


# def _format_datetime_value(value: str):
#     if value.isdigit():
#         if len(value) == 8:
#             value = f"{value[0:4]}-{value[4:6]}-{value[6:8]}"
#         elif len(value) == 12:
#             value = f"{value[0:4]}-{value[4:6]}-{value[6:8]} {value[8:10]}:{value[10:12]}"
#     else:
#         return value


# def _format_date(value: str) -> str:
#     value = _format_datetime_value(value)
#     logging.info(f"format datetime value: {value}")
#     value_datetime = dateparser.parse(value)
#     if not value_datetime:
#         return None
#     return value_datetime.strftime("%Y-%m-%d")


# def _format_datetime(value: str) -> str:
#     value = _format_datetime_value(value)
#     logging.info(f"format datetime value: {value}")
#     value_datetime = dateparser.parse(value)
#     if not value_datetime:
#         return None
#     value_datetime = (
#         value_datetime.replace(tzinfo=timezone.utc).isoformat()
#         if value_datetime.tzname() is None
#         else value_datetime.isoformat()
#     )
#     return value_datetime


# def _format_period(period: dict) -> Tuple[str, str]:
#     start = period.get("start", "")
#     end = period.get("end", "")
#     if start:
#         start = _format_datetime(start)
#     if end:
#         end = _format_datetime(end)

#     if start and not end:
#         end = start
#     elif end and not start:
#         start = end
#     return start, end


# def _update_reference_id_old(
#     resource_type: str,
#     resource: dict,
#     reference_ids: str,
#     field: str,
#     default_id: str = None,
#     is_required: bool = False,
# ):
#     resource_field = resource.get(field)
#     if resource_field is None:
#         resource.update({field: {}})
#         resource_field = resource.get(field)
#     reference_ids = json.loads(reference_ids) if not _is_empty(reference_ids) else {}

#     reference_id = ""
#     if default_id:
#         reference_id = default_id
#     elif resource_field:
#         reference_id_exists = bool(resource_field.get("id"))
#         # TODO: Update to removeprefix after upgrading to python 3.9
#         old_reference_id = (
#             resource_field.get("id")
#             if reference_id_exists
#             else resource_field.get("reference").replace(f"{resource_type}/", "")
#         )
#         reference_id = reference_ids.get(old_reference_id)
#     elif is_required and len(reference_ids) > 0:
#         reference_id = [*reference_ids.values()][0]

#     if reference_id:
#         resource_field.update(
#             {"id": reference_id, "reference": f"{resource_type}/{reference_id}", "type": resource_type}
#         )


# def _update_reference_id(
#     resource_type: str,
#     resource: dict,
#     reference_ids: str,
#     field: str,
# ):
#     resource_field = resource.get(field)
#     if not resource_field:
#         return
#     resource_field_str = json.dumps(resource_field)
#     reference_ids = json.loads(reference_ids) if not _is_empty(reference_ids) else {}
#     for temp_id, actual_id in reference_ids.items():
#         resource_field_str = resource_field_str.replace(f"{resource_type}/{temp_id}", f"{resource_type}/{actual_id}")
#     resource.update({field: json.loads(resource_field_str)})


# def _check_resource_exists(bundle_stats: str, resource_type: str):
#     bundle_stats = json.loads(bundle_stats) if not _is_empty(bundle_stats) else {}
#     if not bundle_stats.get(resource_type):
#         raise AirflowSkipException(f"No entries for {resource_type} resource, Skipping the task.")


# def _validate_resource_identifier(tenant: str, resource: dict):
#     identifiers = resource.get("identifier", [])
#     resource_type = str(resource.get("resourceType"))
#     if len(identifiers) == 0 and resource_type in (
#         ResourceType.Organization.value,
#         ResourceType.Patient.value,
#         ResourceType.Practitioner.value,
#         ResourceType.Coverage.value,
#         ResourceType.Claim.value,
#     ):
#         publish_error_code(PatientDagsErrorCodes.FHIRBUNDLE_RESOURCE_IDENTIFIER_EMPTY.value)
#         raise AirflowFailException(f"{resource_type} resource should have atleast one identifier")
#     for identifier in identifiers:
#         # update coding for standard identifiers like SSN, NPI, TIN if it's not exists.
#         coding = identifier.get("coding", [])
#         system = identifier.get("system")
#         if len(coding) == 0 and system:
#             # SSN
#             if system == "http://hl7.org/fhir/sid/us-ssn" or "2.16.840.1.113883.4.1" in system:
#                 coding = {
#                     "coding": [
#                         {
#                             "code": "SS",
#                             "display": "Social Security Number",
#                             "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
#                         }
#                     ]
#                 }
#                 identifier.update(coding)
#             # NPI
#             elif system == "http://hl7.org/fhir/sid/us-npi" or "2.16.840.1.113883.4.6" in system:
#                 coding = {
#                     "coding": [
#                         {
#                             "code": "NPI",
#                             "display": "National Provider Identifier",
#                             "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
#                         }
#                     ]
#                 }
#                 identifier.update(coding)
#             # TIN
#             elif "2.16.840.1.113883.4.4" in system:
#                 coding = {
#                     "coding": [
#                         {
#                             "code": "TAX",
#                             "display": "TAX Identifier Number",
#                             "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
#                         }
#                     ]
#                 }
#                 identifier.update(coding)
#             else:
#                 coding = {
#                 "coding": [
#                         {
#                             "code": "RI",
#                             "display": "Resource Identifier",
#                             "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
#                         }
#                     ]
#                 }
#                 identifier.update(coding)
#         elif len(coding) == 0 and not system:
#             coding = {
#                 "coding": [
#                     {
#                         "code": "RI",
#                         "display": "Resource Identifier",
#                         "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
#                     }
#                 ]
#             }
#             identifier.update(coding)
#         # # update system based on coding if systen is not exists
#         # elif len(coding) > 0 and not system:
#         #     code = str(coding[0].get("code", ""))
#         #     if code.upper() == "MB":
#         #         identifier["system"] = f"http://healthec.com/{tenant}/member_id"
#         #     elif code.upper() == "SB":
#         #         identifier["system"] = f"http://healthec.com/{tenant}/member_mbi"
#         #     elif code.upper() == "MA":
#         #         identifier["system"] = f"http://healthec.com/{tenant}/member_medicaid_id"
#         #     elif code.upper() == "SN":
#         #         identifier["system"] = f"http://healthec.com/{tenant}/subscriber_id"
#         #     elif code.upper() == "SB":
#         #         identifier["system"] = f"http://healthec.com/{tenant}/member_id"
#         # elif not system:
#         #     system = f"http://healthec.com/{tenant}/{resource_type.lower()}/internal_id"
#         #     logging.info(f"Resource identifier does not have system. Adding auto generated system `{system}`")
#         #     identifier["system"] = system


# def _validate_datetime_field(resource: dict, field: str):
#     datetime_value = _format_datetime(resource.get(field)) if resource.get(field) else ""
#     if datetime_value:
#         resource.update({field: datetime_value})


# def _validate_date_field(resource: dict, field: str):
#     date_value = _format_date(resource.get(field)) if resource.get(field) else ""
#     if date_value:
#         resource.update({field: date_value})


# def _validate_period_field(resource: dict, field: str):
#     if resource.get(field):
#         period_start, period_end = _format_period(resource.get(field))
#         resource.update({field: {"start": period_start, "end": period_end}})


# def _validate_organization_name(resource):
#     if not resource.get("name"):
#         publish_error_code(PatientDagsErrorCodes.FHIRBUNDLE_ORGANIZATION_NAME_EMPTY.value)
#         raise AirflowFailException("Organization name should not be empty or null")


# def _validate_organization_type(resource):
#     if not resource.get("type"):
#         logging.info("Organization type is not provided. Adding default organization type `others`.")
#         resource.update(
#             {
#                 "type": [
#                     {
#                         "coding": [
#                             {
#                                 "code": "other",
#                                 "display": "Other",
#                                 "system": "http://terminology.hl7.org/CodeSystem/organization-type",
#                             }
#                         ]
#                     }
#                 ]
#             }
#         )


# def _validate_organization_tin(resource):
#     type = ""
#     if resource.get("type") and resource["type"].get("coding", []):
#         type = resource["type"]["coding"][0]["code"]
#     if type == "prov":
#         identifiers = resource.get("identifier", [])
#         for identifier in identifiers:
#             if identifier.get("coding", []) and identifier["coding"][0]["code"] == "TAX":
#                 return
#         publish_error_code(PatientDagsErrorCodes.FHIRBUNDLE_ORGANIZATION_TIN_EMPTY.value)
#         raise AirflowFailException("Organization with type `prov` should have Tax identifier")


# def _validate_patient_race_ethnicity(resource):
#     if not resource.get("extension"):
#         resource.update({"extension": []})
#     extensions = resource.get("extension")
#     has_race = False
#     has_ethnicity = False
#     for extension in extensions:
#         if extension.get("url") == "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race":
#             has_race = True
#         elif extension.get("url") == "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity":
#             has_ethnicity = True
#     # add "unknown" race if it does not exists
#     if not has_race:
#         resource["extension"].append(
#             {
#                 "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
#                 "extension": [
#                     {
#                         "system": "http://terminology.hl7.org/CodeSystem/v3-Race",
#                         "url": "ombCategory",
#                         "valueCoding": {
#                             "code": "UNK",
#                             "display": "unknown",
#                             "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
#                         },
#                     },
#                     {
#                         "url": "text",
#                         "valueString": "unknown",
#                     },
#                 ],
#             }
#         )
#     # add "unknown" ethnicity if it does not exists
#     if not has_ethnicity:
#         resource["extension"].append(
#             {
#                 "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
#                 "extension": [
#                     {
#                         "system": "http://terminology.hl7.org/CodeSystem/v3-Ethnicity",
#                         "url": "ombCategory",
#                         "valueCoding": {
#                             "code": "UNK",
#                             "display": "unknown",
#                             "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
#                         },
#                     },
#                     {
#                         "url": "text",
#                         "valueString": "unknown",
#                     },
#                 ],
#             }
#         )


# def _validate_human_name(resource: dict):
#     COMMA_SYMBOL = ","
#     if len(resource.get("name", [])) == 0:
#         publish_error_code(PatientDagsErrorCodes.FHIRBUNDLE_PERSON_NAME_EMPTY.value)
#         raise AirflowFailException("Name should not be empty or null")
#     names = resource.get("name", [])
#     for name in names:
#         if COMMA_SYMBOL in name.get("family") and len(name.get("given")) == 0:
#             name["given"] = [name.get("family").split(",")[0]]
#             name["family"] = name.get("family").split(",")[1]
#         # raise exception if firstname does not exists
#         if len(name.get("given")):
#             publish_error_code(PatientDagsErrorCodes.FHIRBUNDLE_PERSON_NAME_EMPTY.value)
#             raise AirflowFailException("Name should not be empty or null")


# def process_organization(bundle_stats: str, file_tenant: str, file_path: str):
#     _check_resource_exists(bundle_stats, ResourceType.Organization.value)
#     entity_ids = {}
#     with open(file_path, "rb") as f:
#         for entry in ijson.items(f, "entry.item"):
#             resource = entry.get("resource")
#             if resource and resource.get("resourceType") == ResourceType.Organization.value:
#                 temp_id = resource.pop("id")
#                 logging.info(f"Processing organization resource with temporary id {temp_id}")
#                 _validate_resource_identifier(file_tenant, resource)
#                 _validate_organization_name(resource)
#                 _validate_organization_type(resource)
#                 _validate_organization_tin(resource)
#                 actual_id = post_core_entity(file_tenant, resource, Organization)
#                 entity_ids.update({temp_id: actual_id})
#     logging.info(f"entity_ids: {entity_ids}")
#     return entity_ids


# def process_location(bundle_stats: str, file_tenant: str, file_path: str, organization_ids: str):
#     _check_resource_exists(bundle_stats, ResourceType.Location.value)
#     entity_ids = {}
#     with open(file_path, "rb") as f:
#         for entry in ijson.items(f, "entry.item"):
#             resource = entry.get("resource")
#             if resource and resource.get("resourceType") == ResourceType.Location.value:
#                 temp_id = resource.pop("id")
#                 logging.info(f"Processing location resource with temporary id {temp_id}")
#                 _validate_resource_identifier(file_tenant, resource)
#                 _update_reference_id(
#                     ResourceType.Organization.value, resource, organization_ids, "managingOrganization"
#                 )
#                 actual_id = post_organization_sub_entity(file_tenant, resource, "managingOrganization", Location)
#                 entity_ids.update({temp_id: actual_id})
#     logging.info(f"entity_ids: {entity_ids}")
#     return entity_ids


# def process_practitioner(bundle_stats: str, file_tenant: str, file_path: str):
#     _check_resource_exists(bundle_stats, ResourceType.Practitioner.value)
#     practitioner_entries = []
#     with open(file_path, "rb") as f:
#         for entry in ijson.items(f, "entry.item"):
#             practitioner_entries.append(entry)
#     # merge practitioner if we have multiple practitioner with same full name
#     final_practitioner_entries, practitioner_merge_ids = merge_practitioners(practitioner_entries)

#     entity_ids = {}
#     with open(file_path, "rb") as f:
#         for entry in ijson.items(f, "entry.item"):
#             resource = entry.get("resource")
#             if resource and resource.get("resourceType") == ResourceType.Practitioner.value:
#                 temp_id = resource.pop("id")
#                 logging.info(f"Processing practitioner resource with temporary id {temp_id}")
#                 _validate_resource_identifier(file_tenant, resource)
#                 _validate_human_name(resource)
#                 _validate_date_field(resource, "birthDate")
#                 # TODO: check NPI, email
#                 actual_id = post_core_entity(file_tenant, resource, Practitioner)
#                 entity_ids.update({temp_id: actual_id})
#     logging.info(f"entity_ids: {entity_ids}")
#     return entity_ids


# def process_insurance_plan(bundle_stats: str, file_tenant: str, file_path: str, organization_ids: str):
#     _check_resource_exists(bundle_stats, ResourceType.InsurancePlan.value)
#     entity_ids = {}
#     with open(file_path, "rb") as f:
#         for entry in ijson.items(f, "entry.item"):
#             resource = entry.get("resource")
#             if resource and resource.get("resourceType") == ResourceType.InsurancePlan.value:
#                 temp_id = resource.pop("id")
#                 logging.info(f"Processing insurance plan resource with temporary id {temp_id}")
#                 _validate_resource_identifier(file_tenant, resource)
#                 _validate_period_field(resource, "period")
#                 _update_reference_id(ResourceType.Organization.value, resource, organization_ids, "ownedBy")
#                 actual_id = post_core_entity(file_tenant, resource, InsurancePlan)
#                 entity_ids.update({temp_id: actual_id})
#     logging.info(f"entity_ids: {entity_ids}")
#     return entity_ids


# def process_medication(bundle_stats: str, file_tenant: str, file_path: str):
#     _check_resource_exists(bundle_stats, ResourceType.Medication.value)
#     entity_ids = {}
#     with open(file_path, "rb") as f:
#         for entry in ijson.items(f, "entry.item"):
#             resource = entry.get("resource")
#             if resource and resource.get("resourceType") == ResourceType.Medication.value:
#                 temp_id = resource.pop("id")
#                 logging.info(f"Processing medication resource with temporary id {temp_id}")
#                 _validate_resource_identifier(file_tenant, resource)
#                 actual_id = post_core_entity(file_tenant, resource, Medication)
#                 entity_ids.update({temp_id: actual_id})
#     logging.info(f"entity_ids: {entity_ids}")
#     return entity_ids


# def process_patient(bundle_stats: str, file_tenant: str, file_path: str, organization_ids: str, practitioner_ids: str):
#     _check_resource_exists(bundle_stats, ResourceType.Patient.value)
#     entity_ids = {}
#     with open(file_path, "rb") as f:
#         for entry in ijson.items(f, "entry.item"):
#             resource = entry.get("resource")
#             if resource and resource.get("resourceType") == ResourceType.Patient.value:
#                 temp_id = resource.pop("id")
#                 logging.info(f"Processing patient resource with temporary id {temp_id}")
#                 _validate_resource_identifier(file_tenant, resource)
#                 _validate_human_name(resource)
#                 _validate_date_field(resource, "birthDate")
#                 _validate_patient_race_ethnicity(resource)
#                 _update_reference_id(
#                     ResourceType.Organization.value, resource, organization_ids, "managingOrganization"
#                 )
#                 _update_reference_id(
#                     ResourceType.Practitioner.value, resource, practitioner_ids, "generalPractitioner"
#                 )
#                 actual_id = post_core_entity(file_tenant, resource, Patient)
#                 entity_ids.update({temp_id: actual_id})
#     logging.info(f"entity_ids: {entity_ids}")
#     return entity_ids


# def process_practitioner_role(
#     bundle_stats: str,
#     file_tenant: str,
#     file_path: str,
#     organization_ids: str,
#     practitioner_ids: str,
# ):
#     _check_resource_exists(bundle_stats, ResourceType.PractitionerRole.value)
#     entity_ids = {}
#     with open(file_path, "rb") as f:
#         for entry in ijson.items(f, "entry.item"):
#             resource = entry.get("resource")
#             if resource and resource.get("resourceType") == ResourceType.PractitionerRole.value:
#                 temp_id = resource.pop("id")
#                 logging.info(f"Processing practitioner role resource with temporary id {temp_id}")
#                 _validate_resource_identifier(file_tenant, resource)
#                 _validate_period_field(resource, "period")
#                 _update_reference_id(
#                     ResourceType.Organization.value, resource, organization_ids, "organization", None, False
#                 )
#                 _update_reference_id(
#                     ResourceType.Practitioner.value, resource, practitioner_ids, "practitioner", None, False
#                 )
#                 actual_id = post_practitioner_sub_entity(file_tenant, resource, "practitioner", PractitionerRole)
#                 entity_ids.update({temp_id: actual_id})
#     logging.info(f"entity_ids: {entity_ids}")
#     return entity_ids


# def process_coverage(
#     bundle_stats: str,
#     file_tenant: str,
#     file_path: str,
#     organization_ids: str,
#     patient_ids: str,
#     insurance_plan_ids: str,
# ):
#     _check_resource_exists(bundle_stats, ResourceType.Coverage.value)
#     entity_ids = {}
#     with open(file_path, "rb") as f:
#         for entry in ijson.items(f, "entry.item"):
#             resource = entry.get("resource")
#             temp_id = resource.pop("id")
#             logging.info(f"Processing coverage resource with temporary id {temp_id}")
#             if resource and resource.get("resourceType") == ResourceType.Coverage.value:
#                 _validate_resource_identifier(file_tenant, resource)
#                 _validate_period_field(resource, "period")
#                 _update_reference_id(
#                     ResourceType.Organization.value, resource, organization_ids, "insurer", None, False
#                 )
#                 _update_reference_id(ResourceType.Patient.value, resource, patient_ids, "beneficiary", None, False)
#                 _update_reference_id(ResourceType.Patient.value, resource, patient_ids, "subscriber", None, False)
#                 _update_reference_id(
#                     ResourceType.InsurancePlan.value, resource, insurance_plan_ids, "insurancePlan", None, False
#                 )
#                 actual_id = post_core_entity(file_tenant, resource, Coverage)
#                 entity_ids.update({temp_id: actual_id})
#     logging.info(f"entity_ids: {entity_ids}")
#     return entity_ids


# def process_encounter(
#     bundle_stats: str,
#     file_tenant: str,
#     file_path: str,
#     patient_ids: str,
# ):
#     _check_resource_exists(bundle_stats, ResourceType.Encounter.value)
#     entity_ids = {}
#     with open(file_path, "rb") as f:
#         for entry in ijson.items(f, "entry.item"):
#             resource = entry.get("resource")
#             temp_id = resource.pop("id")
#             logging.info(f"Processing encounter resource with temporary id {temp_id}")
#             if resource and resource.get("resourceType") == ResourceType.Encounter.value:
#                 _validate_resource_identifier(file_tenant, resource)
#                 _validate_period_field(resource, "period")
#                 _update_reference_id(
#                     ResourceType.Patient.value, resource, patient_ids, "subject", None, False
#                 )
#                 actual_id = post_patient_sub_entity(file_tenant, resource, "subject", Encounter)
#                 entity_ids.update({temp_id: actual_id})
#     logging.info(f"entity_ids: {entity_ids}")
#     return entity_ids


# def process_condition(
#     bundle_stats: str,
#     file_tenant: str,
#     file_path: str,
#     patient_ids: str,
#     encounter_ids: str,
# ):
#     _check_resource_exists(bundle_stats, ResourceType.Condition.value)
#     entity_ids = {}
#     with open(file_path, "rb") as f:
#         for entry in ijson.items(f, "entry.item"):
#             resource = entry.get("resource")
#             temp_id = resource.pop("id")
#             logging.info(f"Processing condition resource with temporary id {temp_id}")
#             if resource and resource.get("resourceType") == ResourceType.Condition.value:
#                 _validate_resource_identifier(file_tenant, resource)
#                 _validate_datetime_field(resource, "onsetDateTime")
#                 _validate_period_field(resource, "onsetPeriod")
#                 _validate_datetime_field(resource, "abatementDateTime")
#                 _validate_period_field(resource, "abatementPeriod")
#                 _update_reference_id(ResourceType.Patient.value, resource, patient_ids, "subject", None, False)
#                 _update_reference_id(ResourceType.Encounter.value, resource, encounter_ids, "encounter", None, False)
#                 actual_id = post_patient_sub_entity(file_tenant, resource, "subject", Condition)
#                 entity_ids.update({temp_id: actual_id})
#     logging.info(f"entity_ids: {entity_ids}")
#     return entity_ids


# def process_procedure(
#     bundle_stats: str,
#     file_tenant: str,
#     file_path: str,
#     patient_ids: str,
#     encounter_ids: str,
# ):
#     _check_resource_exists(bundle_stats, ResourceType.Procedure.value)
#     entity_ids = {}
#     with open(file_path, "rb") as f:
#         for entry in ijson.items(f, "entry.item"):
#             resource = entry.get("resource")
#             temp_id = resource.pop("id")
#             logging.info(f"Processing procedure resource with temporary id {temp_id}")
#             if resource and resource.get("resourceType") == ResourceType.Procedure.value:
#                 _validate_resource_identifier(file_tenant, resource)
#                 _validate_datetime_field(resource, "performedDateTime")
#                 _validate_period_field(resource, "performedPeriod")
#                 _update_reference_id(ResourceType.Patient.value, resource, patient_ids, "subject", None, False)
#                 _update_reference_id(ResourceType.Encounter.value, resource, encounter_ids, "encounter", None, False)
#                 actual_id = post_patient_sub_entity(file_tenant, resource, "subject", Procedure)
#                 entity_ids.update({temp_id: actual_id})
#     logging.info(f"entity_ids: {entity_ids}")
#     return entity_ids


# def process_observation(
#     bundle_stats: str,
#     file_tenant: str,
#     file_path: str,
#     patient_ids: str,
#     encounter_ids: str,
# ):
#     _check_resource_exists(bundle_stats, ResourceType.Observation.value)
#     entity_ids = {}
#     with open(file_path, "rb") as f:
#         for entry in ijson.items(f, "entry.item"):
#             resource = entry.get("resource")
#             temp_id = resource.pop("id")
#             logging.info(f"Processing observation resource with temporary id {temp_id}")
#             if resource and resource.get("resourceType") == ResourceType.Observation.value:
#                 _validate_resource_identifier(file_tenant, resource)
#                 _validate_datetime_field(resource, "effectiveDateTime")
#                 _validate_period_field(resource, "effectivePeriod")
#                 _update_reference_id(ResourceType.Patient.value, resource, patient_ids, "subject", None, False)
#                 _update_reference_id(ResourceType.Encounter.value, resource, encounter_ids, "encounter", None, False)
#                 actual_id = post_patient_sub_entity(file_tenant, resource, "subject", Observation)
#                 entity_ids.update({temp_id: actual_id})
#     logging.info(f"entity_ids: {entity_ids}")
#     return entity_ids


# def process_allergy_intolerance(
#     bundle_stats: str,
#     file_tenant: str,
#     file_path: str,
#     patient_ids: str,
#     encounter_ids: str,
# ):
#     _check_resource_exists(bundle_stats, ResourceType.AllergyIntolerance.value)
#     entity_ids = {}
#     with open(file_path, "rb") as f:
#         for entry in ijson.items(f, "entry.item"):
#             resource = entry.get("resource")
#             temp_id = resource.pop("id")
#             logging.info(f"Processing allergy intolerance resource with temporary id {temp_id}")
#             if resource and resource.get("resourceType") == ResourceType.AllergyIntolerance.value:
#                 _validate_resource_identifier(file_tenant, resource)
#                 _validate_datetime_field(resource, "onsetDateTime")
#                 _validate_period_field(resource, "onsetPeriod")
#                 _update_reference_id(ResourceType.Patient.value, resource, patient_ids, "patient", None, False)
#                 _update_reference_id(ResourceType.Encounter.value, resource, encounter_ids, "encounter", None, False)
#                 actual_id = post_patient_sub_entity(file_tenant, resource, "patient", AllergyIntolerance)
#                 entity_ids.update({temp_id: actual_id})
#     logging.info(f"entity_ids: {entity_ids}")
#     return entity_ids


# def process_questionnaire_response(
#     bundle_stats: str,
#     file_tenant: str,
#     file_path: str,
#     patient_ids: str,
#     encounter_ids: str,
#     practitioner_ids: str,
# ):
#     _check_resource_exists(bundle_stats, ResourceType.QuestionnaireResponse.value)
#     entity_ids = {}
#     with open(file_path, "rb") as f:
#         for entry in ijson.items(f, "entry.item"):
#             resource = entry.get("resource")
#             temp_id = resource.pop("id")
#             logging.info(f"Processing questionnaire response resource with temporary id {temp_id}")
#             if resource and resource.get("resourceType") == ResourceType.QuestionnaireResponse.value:
#                 _validate_resource_identifier(file_tenant, resource)
#                 _update_reference_id(ResourceType.Patient.value, resource, patient_ids, "subject", None, False)
#                 _update_reference_id(ResourceType.Encounter.value, resource, encounter_ids, "encounter", None, False)
#                 _update_reference_id(
#                     ResourceType.Practitioner.value, resource, practitioner_ids, "author", None, False
#                 )
#                 actual_id = post_patient_sub_entity(file_tenant, resource, "subject", QuestionnaireResponse)
#                 entity_ids.update({temp_id: actual_id})
#     logging.info(f"entity_ids: {entity_ids}")
#     return entity_ids


# def process_medication_statement(
#     bundle_stats: str,
#     file_tenant: str,
#     file_path: str,
#     patient_ids: str,
#     encounter_ids: str,
#     medication_ids: str,
# ):
#     _check_resource_exists(bundle_stats, ResourceType.MedicationStatement.value)
#     entity_ids = {}
#     with open(file_path, "rb") as f:
#         for entry in ijson.items(f, "entry.item"):
#             resource = entry.get("resource")
#             temp_id = resource.pop("id")
#             logging.info(f"Processing medication statement resource with temporary id {temp_id}")
#             if resource and resource.get("resourceType") == ResourceType.MedicationStatement.value:
#                 _validate_resource_identifier(file_tenant, resource)
#                 _validate_datetime_field(resource, "effectiveDateTime")
#                 _validate_period_field(resource, "effectivePeriod")
#                 _update_reference_id(ResourceType.Patient.value, resource, patient_ids, "subject", None, False)
#                 _update_reference_id(ResourceType.Encounter.value, resource, encounter_ids, "context", None, False)
#                 _update_reference_id(
#                     ResourceType.Medication.value, resource, medication_ids, "medicationReference", None, False
#                 )
#                 actual_id = post_patient_sub_entity(file_tenant, resource, "subject", MedicationStatement)
#                 entity_ids.update({temp_id: actual_id})
#     logging.info(f"entity_ids: {entity_ids}")
#     return entity_ids


# def process_claim(
#     bundle_stats: str,
#     file_tenant: str,
#     file_path: str,
#     patient_ids: str,
# ):
#     _check_resource_exists(bundle_stats, ResourceType.Claim.value)
#     entity_ids = {}
#     with open(file_path, "rb") as f:
#         for entry in ijson.items(f, "entry.item"):
#             resource = entry.get("resource")
#             temp_id = resource.pop("id")
#             logging.info(f"Processing claim resource with temporary id {temp_id}")
#             if resource and resource.get("resourceType") == ResourceType.Claim.value:
#                 _validate_resource_identifier(file_tenant, resource)
#                 _validate_period_field(resource, "billablePeriod")
#                 _validate_datetime_field(resource, "created")
#                 _update_reference_id(ResourceType.Patient.value, resource, patient_ids, "patient", None, False)
#                 actual_id = post_core_entity(file_tenant, resource, Claim)
#                 entity_ids.update({temp_id: actual_id})
#     logging.info(f"entity_ids: {entity_ids}")
#     return entity_ids


# def process_claim_response(
#     bundle_stats: str,
#     file_tenant: str,
#     file_path: str,
#     patient_ids: str,
# ):
#     _check_resource_exists(bundle_stats, ResourceType.ClaimResponse.value)
#     entity_ids = {}
#     with open(file_path, "rb") as f:
#         for entry in ijson.items(f, "entry.item"):
#             resource = entry.get("resource")
#             temp_id = resource.pop("id")
#             logging.info(f"Processing claim response resource with temporary id {temp_id}")
#             if resource and resource.get("resourceType") == ResourceType.ClaimResponse.value:
#                 _validate_resource_identifier(file_tenant, resource)
#                 _validate_datetime_field(resource, "created")
#                 _update_reference_id(ResourceType.Patient.value, resource, patient_ids, "patient", None, False)
#                 actual_id = post_core_entity(file_tenant, resource, ClaimResponse)
#                 entity_ids.update({temp_id: actual_id})
#     logging.info(f"entity_ids: {entity_ids}")
#     return entity_ids
