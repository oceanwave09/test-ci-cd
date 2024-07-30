from enum import Enum


class ResourceType(Enum):
    Organization = "Organization"
    Location = "Location"
    Practitioner = "Practitioner"
    PractitionerRole = "PractitionerRole"
    Patient = "Patient"
    Encounter = "Encounter"
    Procedure = "Procedure"
    Condition = "Condition"
    Observation = "Observation"
    DocumentReference = "DocumentReference"
    InsurancePlan = "InsurancePlan"
    Coverage = "Coverage"
    Medication = "Medication"
    AllergyIntolerance = "AllergyIntolerance"
    QuestionnaireResponse = "QuestionnaireResponse"
    MedicationRequest = "MedicationRequest"
    MedicationStatement = "MedicationStatement"
    Claim = "Claim"
    ClaimResponse = "ClaimResponse"
    Immunization = "Immunization"
    ServiceRequest = "ServiceRequest"


class MatchingResourceType(Enum):
    Organization = "Organization"
    Location = "Location"
    Patient = "Patient"
    Practitioner = "Practitioner"
    Encounter = "Encounter"
    Procedure = "Procedure"
    Condition = "Condition"
    Observation = "Observation"
    Immunization = "Immunization"
    AllergyIntolerance = "AllergyIntolerance"
    InsurancePlan = "InsurancePlan"
    Coverage = "Coverage"
    Medication = "Medication"
    Claim = "Claim"


class UpsertFileType(Enum):
    NCQA_HEDIS_MEMBERGM = "NCQA_HEDIS_MEMBERGM"
