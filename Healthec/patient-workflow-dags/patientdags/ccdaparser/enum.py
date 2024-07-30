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
