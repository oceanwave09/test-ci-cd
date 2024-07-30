from enum import Enum


class ClaimType(str, Enum):
    INSTITUTIONAL = "institutional"
    PROFESSIONAL = "professional"
    DME = "dme"
    DENTAL = "dental"
    PHARMACY = "pharmacy"

    def __str__(self) -> str:
        return self.value


class ResourceType(str, Enum):
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

    def __str__(self) -> str:
        return self.value
