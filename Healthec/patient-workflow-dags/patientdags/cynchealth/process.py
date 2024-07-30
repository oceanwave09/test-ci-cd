import csv
import json
import os

import smart_open
from airflow.exceptions import AirflowFailException
from fhirclient.resources.condition import Condition
from fhirclient.resources.encounter import Encounter
from fhirclient.resources.immunization import Immunization
from fhirclient.resources.observation import Observation
from fhirclient.resources.organization import Organization
from fhirclient.resources.patient import Patient
from fhirclient.resources.practitioner import Practitioner
from fhirclient.resources.procedure import Procedure
from fhirtransformer.transformer import FHIRTransformer

from patientdags.cynchealth.constants import PROCESSED_PATH_SUFIX
from patientdags.cynchealth.transform import (
    tranform_practitioner,
    transform_condition,
    transform_encounter,
    transform_immunization,
    transform_observation,
    transform_organization,
    transform_patient,
    transform_procedure,
)
from patientdags.cynchealth.utils import move_file_in_s3
from patientdags.utils.api_client import (
    match_resource_entity,
    post_resource_entity,
    upsert_resource_entity,
)
from patientdags.utils.enum import ResourceType
from patientdags.utils.error_codes import PatientDagsErrorCodes, publish_error_code


def resolve_reference(ref_ids: dict, src_id: str = None, default: bool = False) -> str:
    ref_id = ""
    if src_id:
        ref_id = ref_ids.get(src_id, "")
    if not ref_id and len(ref_ids) > 0 and default:
        ref_id = next(iter(ref_ids.values()))
    return ref_id


def process_organization(file_tenant: str, file_path: str, file_groups: str) -> dict:
    try:
        org_ids = {}
        transformer = FHIRTransformer()
        transformer.load_template("organization.j2")
        file_groups = json.loads(file_groups)
        files = file_groups.get(ResourceType.Organization.value, [])
        files.sort()
        print(f"process_organization - files: {files}")
        for file in files:
            with smart_open.open(file, "r") as f:
                for entry in csv.DictReader(f):
                    resource = transform_organization(entry, transformer)
                    print(f"process_organization - resource: {resource}")
                    resource_id = match_resource_entity(file_tenant, resource, Organization)
                    print(f"process_organization - match resource_id: {resource_id}")
                    if not resource_id:
                        resource_id = post_resource_entity(file_tenant, resource, Organization)
                        print(f"process_organization - post resource_id: {resource_id}")
                    if resource_id:
                        org_ids.update({entry.get("Identifier"): resource_id})
            file_name = os.path.basename(file)
            processed_file = os.path.join(file_path, PROCESSED_PATH_SUFIX, file_name)
            move_file_in_s3(file, processed_file)
        print(f"process_organization - org_ids: {org_ids}")
        return json.dumps(org_ids)
    except Exception as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_PROCESSING_ERROR.value}: {str(e)}")
        # file_name = os.path.basename(file)
        # error_file = os.path.join(file_path, ERROR_PATH_SUFIX, file_name)
        # move_file_in_s3(file, error_file)
        raise AirflowFailException(e)


def process_practitioner(file_tenant: str, file_path: str, file_groups: str) -> dict:
    try:
        pract_ids = {}
        transformer = FHIRTransformer()
        transformer.load_template("practitioner.j2")
        file_groups = json.loads(file_groups)
        files = file_groups.get(ResourceType.Practitioner.value, [])
        files.sort()
        print(f"process_practitioner - files: {files}")
        for file in files:
            with smart_open.open(file, "r") as f:
                for entry in csv.DictReader(f):
                    resource = tranform_practitioner(entry, transformer)
                    print(f"process_practitioner - resource: {resource}")
                    resource_id = match_resource_entity(file_tenant, resource, Practitioner)
                    print(f"process_practitioner - match resource_id: {resource_id}")
                    if not resource_id:
                        resource_id = post_resource_entity(file_tenant, resource, Practitioner)
                        print(f"process_practitioner - post resource_id: {resource_id}")
                    if resource_id:
                        pract_ids.update({entry.get("Identifier"): resource_id})
            file_name = os.path.basename(file)
            processed_file = os.path.join(file_path, PROCESSED_PATH_SUFIX, file_name)
            move_file_in_s3(file, processed_file)
        print(f"process_practitioner - pract_ids: {pract_ids}")
        return json.dumps(pract_ids)
    except Exception as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_PROCESSING_ERROR.value}: {str(e)}")
        # file_name = os.path.basename(file)
        # error_file = os.path.join(file_path, ERROR_PATH_SUFIX, file_name)
        # move_file_in_s3(file, error_file)
        raise AirflowFailException(e)


def process_patient(file_tenant: str, file_path: str, file_groups: str, org_ids: str) -> dict:
    try:
        pat_ids = {}
        transformer = FHIRTransformer()
        transformer.load_template("patient.j2")
        file_groups = json.loads(file_groups)
        org_ids = json.loads(org_ids)
        files = file_groups.get(ResourceType.Patient.value, [])
        files.sort()
        print(f"process_patient - files: {files}")
        for file in files:
            with smart_open.open(file, "r") as f:
                for entry in csv.DictReader(f):
                    org_id = resolve_reference(org_ids, default=True)
                    print(f"process_patient - org_id: {org_id}")
                    entry["OrganizationRef"] = org_id
                    resource = transform_patient(entry, transformer)
                    print(f"process_patient - resource: {resource}")
                    resource_id = upsert_resource_entity(file_tenant, resource, Patient)
                    print(f"process_patient - resource_id: {resource_id}")
                    pat_ids.update({entry.get("MRN"): resource_id})
            file_name = os.path.basename(file)
            processed_file = os.path.join(file_path, PROCESSED_PATH_SUFIX, file_name)
            move_file_in_s3(file, processed_file)
        print(f"process_patient - pat_ids: {pat_ids}")
        return json.dumps(pat_ids)
    except Exception as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_PROCESSING_ERROR.value}: {str(e)}")
        # file_name = os.path.basename(file)
        # error_file = os.path.join(file_path, ERROR_PATH_SUFIX, file_name)
        # move_file_in_s3(file, error_file)
        raise AirflowFailException(e)


def process_encounter(
    file_tenant: str, file_path: str, file_groups: str, pat_ids: str, pract_ids: str, org_ids: str
) -> dict:
    try:
        enc_ids = {}
        transformer = FHIRTransformer()
        transformer.load_template("encounter.j2")
        file_groups = json.loads(file_groups)
        pat_ids = json.loads(pat_ids)
        pract_ids = json.loads(pract_ids)
        org_ids = json.loads(org_ids)
        files = file_groups.get(ResourceType.Encounter.value, [])
        files.sort()
        print(f"process_encounter - files: {files}")
        for file in files:
            with smart_open.open(file, "r") as f:
                for entry in csv.DictReader(f):
                    pat_id = resolve_reference(pat_ids, entry.get("MRN"))
                    print(f"process_encounter - pat_id: {pat_id}")
                    pract_id = resolve_reference(pract_ids, entry.get("Practitioner"))
                    print(f"process_encounter - pract_id: {pract_id}")
                    org_id = resolve_reference(org_ids, entry.get("Organization"), default=True)
                    print(f"process_encounter - org_id: {org_id}")
                    entry["PatientRef"] = pat_id
                    entry["PractitionerRef"] = pract_id
                    entry["OrganizationRef"] = org_id
                    resource = transform_encounter(entry, transformer)
                    print(f"process_encounter - resource: {resource}")
                    resource_id = upsert_resource_entity(file_tenant, resource, Encounter)
                    print(f"process_encounter - resource_id: {resource_id}")
                    enc_ids.update({entry.get("ID"): resource_id})
            file_name = os.path.basename(file)
            processed_file = os.path.join(file_path, PROCESSED_PATH_SUFIX, file_name)
            move_file_in_s3(file, processed_file)
        print(f"process_encounter - enc_ids: {enc_ids}")
        return json.dumps(enc_ids)
    except Exception as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_PROCESSING_ERROR.value}: {str(e)}")
        # file_name = os.path.basename(file)
        # error_file = os.path.join(file_path, ERROR_PATH_SUFIX, file_name)
        # move_file_in_s3(file, error_file)
        raise AirflowFailException(e)


def process_condition(
    file_tenant: str,
    file_path: str,
    file_groups: str,
    pat_ids: str,
    enc_ids: str,
) -> list:
    try:
        con_ids = []
        transformer = FHIRTransformer()
        transformer.load_template("condition.j2")
        file_groups = json.loads(file_groups)
        pat_ids = json.loads(pat_ids)
        enc_ids = json.loads(enc_ids)
        files = file_groups.get(ResourceType.Condition.value, [])
        files.sort()
        print(f"process_condition - files: {files}")
        for file in files:
            with smart_open.open(file, "r") as f:
                for entry in csv.DictReader(f):
                    pat_id = resolve_reference(pat_ids, entry.get("MRN"))
                    print(f"process_condition - pat_id: {pat_id}")
                    enc_id = resolve_reference(enc_ids, entry.get("EncounterID"))
                    print(f"process_condition - enc_id: {enc_id}")
                    entry["PatientRef"] = pat_id
                    entry["EncounterRef"] = enc_id
                    resource = transform_condition(entry, transformer)
                    print(f"process_condition - resource: {resource}")
                    resource_id = post_resource_entity(file_tenant, resource, Condition, "Patient", pat_id)
                    print(f"process_condition - resource_id: {resource_id}")
                    con_ids.append(resource_id)
            file_name = os.path.basename(file)
            processed_file = os.path.join(file_path, PROCESSED_PATH_SUFIX, file_name)
            move_file_in_s3(file, processed_file)
        print(f"process_condition - con_ids: {con_ids}")
        return con_ids
    except Exception as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_PROCESSING_ERROR.value}: {str(e)}")
        # file_name = os.path.basename(file)
        # error_file = os.path.join(file_path, ERROR_PATH_SUFIX, file_name)
        # move_file_in_s3(file, error_file)
        raise AirflowFailException(e)


def process_observation(
    file_tenant: str,
    file_path: str,
    file_groups: str,
    pat_ids: str,
    enc_ids: str,
) -> list:
    try:
        obs_ids = []
        transformer = FHIRTransformer()
        transformer.load_template("observation.j2")
        file_groups = json.loads(file_groups)
        pat_ids = json.loads(pat_ids)
        enc_ids = json.loads(enc_ids)
        files = file_groups.get(ResourceType.Observation.value, [])
        files.sort()
        print(f"process_observation - files: {files}")
        for file in files:
            with smart_open.open(file, "r") as f:
                for entry in csv.DictReader(f):
                    pat_id = resolve_reference(pat_ids, entry.get("MRN"))
                    print(f"process_observation - pat_id: {pat_id}")
                    enc_id = resolve_reference(enc_ids, entry.get("EncounterID"))
                    print(f"process_observation - enc_id: {enc_id}")
                    entry["PatientRef"] = pat_id
                    entry["EncounterRef"] = enc_id
                    resource = transform_observation(entry, transformer)
                    print(f"process_observation - resource: {resource}")
                    resource_id = post_resource_entity(file_tenant, resource, Observation, "Patient", pat_id)
                    print(f"process_observation - resource_id: {resource_id}")
                    obs_ids.append(resource_id)
            file_name = os.path.basename(file)
            processed_file = os.path.join(file_path, PROCESSED_PATH_SUFIX, file_name)
            move_file_in_s3(file, processed_file)
        print(f"process_observation - obs_ids: {obs_ids}")
        return obs_ids
    except Exception as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_PROCESSING_ERROR.value}: {str(e)}")
        # file_name = os.path.basename(file)
        # error_file = os.path.join(file_path, ERROR_PATH_SUFIX, file_name)
        # move_file_in_s3(file, error_file)
        raise AirflowFailException(e)


def process_procedure(
    file_tenant: str,
    file_path: str,
    file_groups: str,
    pat_ids: str,
    enc_ids: str,
) -> list:
    try:
        proc_ids = []
        transformer = FHIRTransformer()
        transformer.load_template("procedure.j2")
        file_groups = json.loads(file_groups)
        pat_ids = json.loads(pat_ids)
        enc_ids = json.loads(enc_ids)
        files = file_groups.get(ResourceType.Procedure.value, [])
        files.sort()
        print(f"process_procedure - files: {files}")
        for file in files:
            with smart_open.open(file, "r") as f:
                for entry in csv.DictReader(f):
                    pat_id = resolve_reference(pat_ids, entry.get("MRN"))
                    print(f"process_procedure - pat_id: {pat_id}")
                    enc_id = resolve_reference(enc_ids, entry.get("EncounterID"))
                    print(f"process_procedure - enc_id: {enc_id}")
                    entry["PatientRef"] = pat_id
                    entry["EncounterRef"] = enc_id
                    resource = transform_procedure(entry, transformer)
                    print(f"process_procedure - resource: {resource}")
                    resource_id = post_resource_entity(file_tenant, resource, Procedure, "Patient", pat_id)
                    print(f"process_procedure - resource_id: {resource_id}")
                    proc_ids.append(resource_id)
            file_name = os.path.basename(file)
            processed_file = os.path.join(file_path, PROCESSED_PATH_SUFIX, file_name)
            move_file_in_s3(file, processed_file)
        print(f"process_procedure - proc_ids: {proc_ids}")
        return proc_ids
    except Exception as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_PROCESSING_ERROR.value}: {str(e)}")
        # file_name = os.path.basename(file)
        # error_file = os.path.join(file_path, ERROR_PATH_SUFIX, file_name)
        # move_file_in_s3(file, error_file)
        raise AirflowFailException(e)


def process_immunization(
    file_tenant: str,
    file_path: str,
    file_groups: str,
    pat_ids: str,
    enc_ids: str,
) -> list:
    try:
        immu_ids = []
        transformer = FHIRTransformer()
        transformer.load_template("immunization.j2")
        file_groups = json.loads(file_groups)
        pat_ids = json.loads(pat_ids)
        enc_ids = json.loads(enc_ids)
        files = file_groups.get(ResourceType.Immunization.value, [])
        files.sort()
        print(f"process_immunization - files: {files}")
        for file in files:
            with smart_open.open(file, "r") as f:
                for entry in csv.DictReader(f):
                    pat_id = resolve_reference(pat_ids, entry.get("MRN"))
                    print(f"process_immunization - pat_id: {pat_id}")
                    enc_id = resolve_reference(enc_ids, entry.get("EncounterID"))
                    print(f"process_immunization - enc_id: {enc_id}")
                    entry["PatientRef"] = pat_id
                    entry["EncounterRef"] = enc_id
                    resource = transform_immunization(entry, transformer)
                    print(f"process_immunization - resource: {resource}")
                    resource_id = post_resource_entity(file_tenant, resource, Immunization, "Patient", pat_id)
                    print(f"process_immunization - resource_id: {resource_id}")
                    immu_ids.append(resource_id)
            file_name = os.path.basename(file)
            processed_file = os.path.join(file_path, PROCESSED_PATH_SUFIX, file_name)
            move_file_in_s3(file, processed_file)
        print(f"process_immunization - immu_ids: {immu_ids}")
        return immu_ids
    except Exception as e:
        publish_error_code(f"{PatientDagsErrorCodes.RESOURCE_PROCESSING_ERROR.value}: {str(e)}")
        # file_name = os.path.basename(file)
        # error_file = os.path.join(file_path, ERROR_PATH_SUFIX, file_name)
        # move_file_in_s3(file, error_file)
        raise AirflowFailException(e)
