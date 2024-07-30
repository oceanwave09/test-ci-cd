from enum import Enum


class RecordStatus(Enum):
    STAGE_LOAD = "STAGE_LOAD"
    TO_BE_VALIDATED = "TO_BE_VALIDATED"
    TO_BE_PROCESSED = "TO_BE_PROCESSED"
    PROCESSED = "PROCESSED"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    FIELD_VALIDATION_ERROR = "FIELD_VALIDATION_ERROR"
    PRECHECK_VALIDATION_ERROR = "PRECHECK_VALIDATION_ERROR"


class ResourceType(Enum):
    AllergyIntolerance = "AllergyIntolerance"
    Condition = "Condition"
    Coverage = "Coverage"
    Claim = "Claim"
    Communication = "Communication"
    ClaimResponse = "ClaimResponse"
    Encounter = "Encounter"
    ExplanationOfBenefit = "ExplanationOfBenefit"
    FamilyMemberHistory = "FamilyMemberHistory"
    Immunization = "Immunization"
    InsurancePlan = "InsurancePlan"
    Location = "Location"
    Medication = "Medication"
    MedicationRequest = "MedicationRequest"
    MedicationStatement = "MedicationStatement"
    Observation = "Observation"
    Organization = "Organization"
    OrganizationAffiliation = "OrganizationAffiliation"
    Patient = "Patient"
    Practitioner = "Practitioner"
    PractitionerRole = "PractitionerRole"
    Procedure = "Procedure"
    QuestionnaireResponse = "QuestionnaireResponse"
    ServiceRequest = "ServiceRequest"
