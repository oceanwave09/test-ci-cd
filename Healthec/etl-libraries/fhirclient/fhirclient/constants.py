# coding: utf-8

import enum

# Acceptable return codes from domain services
RESPONSE_CODE_200 = 200
RESPONSE_CODE_201 = 201
RESPONSE_CODE_204 = 204
RESPONSE_CODE_409 = 409

# Auth service constants
REFRESH_INTERVAL = 30


# FHIR ResourceType Enum class
class ResourceType(enum.Enum):
    Patient = "Patient"
    RelatedPerson = "RelatedPerson"
    Organization = "Organization"
    OrganizationAffiliation = "OrganizationAffiliation"
    Practitioner = "Practitioner"
    Location = "Location"
    Encounter = "Encounter"
    Condition = "Condition"
    AllergyIntolerance = "AllergyIntolerance"
    Observation = "Observation"
    Procedure = "Procedure"
    Immunization = "Immunization"
    MedicationStatement = "MedicationStatement"
    MedicationAdministration = "MedicationAdministration"
    Medication = "Medication"
    PractitionerRole = "PractitionerRole"
    DiagnosticReport = "DiagnosticReport"
    EpisodeOfCare = "EpisodeOfCare"
    Device = "Device"
    CarePlan = "CarePlan"
    ServiceRequest = "ServiceRequest"
    MedicationRequest = "MedicationRequest"
    MedicationDispense = "MedicationDispense"
    Claim = "Claim"
    Coverage = "Coverage"
    ClaimResponse = "ClaimResponse"
    InsurancePlan = "InsurancePlan"

    def get_resource_path(self, scope: str = None, scope_id: str = None):
        resource_value = f"{str(self.value).lower()}s"
        if self.value == "AllergyIntolerance":
            resource_value = "allergy-intolerances"
        elif self.value == "MedicationStatement":
            resource_value = "medication-statements"
        elif self.value == "PractitionerRole":
            resource_value = "practitioner-roles"
        elif self.value == "MedicationAdministration":
            resource_value = "medication-administrations"
        elif self.value == "DiagnosticReport":
            resource_value = "diagnostic-reports"
        elif self.value == "EpisodeOfCare":
            resource_value = "episodes-of-care"
        elif self.value == "CarePlan":
            resource_value = "care-plans"
        elif self.value == "ServiceRequest":
            resource_value = "service-requests"
        elif self.value == "MedicationRequest":
            resource_value = "medication-requests"
        elif self.value == "MedicationDispense":
            resource_value = "medication-dispenses"
        elif self.value == "Claim":
            resource_value = "claims"
        elif self.value == "Coverage":
            resource_value = "coverages"
        elif self.value == "ClaimResponse":
            resource_value = "claim-responses"
        elif self.value == "InsurancePlan":
            resource_value = "insurance-plans"
        elif self.value == "RelatedPerson":
            resource_value = "related-persons"
        elif self.value == "OrganizationAffiliation":
            resource_value = "organization-affiliations"         
            
        if scope == "Patient":
            return f"patients/{scope_id}/{resource_value}/"
        elif scope == "Organization":
            return f"organizations/{scope_id}/{resource_value}/"
        elif scope == "Practitioner":
            return f"practitioners/{scope_id}/{resource_value}/"

        return f"{resource_value}/"
