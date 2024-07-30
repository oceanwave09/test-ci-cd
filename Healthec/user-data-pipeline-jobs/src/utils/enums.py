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
    Organization = "Organization"
    OrganizationAffiliation = "OrganizationAffiliation"
    Practitioner = "Practitioner"
    PractitionerRole = "PractitionerRole"
    Location = "Location"
