from enum import Enum

XML_SCHEMA = "xsd/cda/CDA_SDTC.xsd"
CCDA_CONFIG = "ccda_config.json"


class InputFormat(str, Enum):
    FILE = "file"


class ReplaceSign(str, Enum):
    AT = "@"
    DOLLAR = "$"


SSN_IDENTIFIER_SYSTEMS = [
    "2.16.840.1.113883.4.1",
    "http://hl7.org/fhir/sid/us-ssn",
]

NPI_IDENTIFIER_SYSTEMS = [
    "2.16.840.1.113883.4.6",
    "http://hl7.org/fhir/sid/us-npi",
]

TAX_IDENTIFIER_SYSTEMS = [
    "2.16.840.1.113883.4.4",
]

# Language Codes
LANGUAGE_CODES = {
    "English": {
        "language_code": "en",
        "language_display": "English",
    }
}

# Race Codes
RACE_CODES = {
    "White": {
        "race_code": "2106-3",
        "race_display": "White",
    },
    "Black or African American": {
        "race_code": "2054-5",
        "race_display": "Black or African American",
    },
}

# Ethnic Group Codes
ETHNICITY_CODES = {
    "Not Hispanic or Latino": {
        "ethnicity_code": "2186-5",
        "ethnicity_display": "Not Hispanic or Latino",
    },
    "Hispanic or Latino": {
        "ethnicity_code": "2135-2",
        "ethnicity_display": "Hispanic or Latino",
    },
}

# Marital Status Codes
MARITAL_STATUS_CODES = {
    "Married": {
        "marital_status_code": "M",
        "marital_status_display": "Married",
    },
    "Unmarried": {
        "marital_status_code": "U",
        "marital_status_display": "Unmarried",
    },
}

# Nationality Codes
NATIONALITY_CODES = {
    "USA": {
        "nationality_code": "USA",
        "nationality_display": "United States of America",
    },
    "CAN": {
        "nationality_code": "CAN",
        "nationality_display": "Canada",
    },
}

# Religion Codes
RELIGION_CODES = {
    "Christian": {
        "religion_code": "1013",
        "religion_display": "Christian",
    },
    "Buddhism": {
        "religion_code": "1059",
        "religion_display": "Zen Buddhism",
    },
}

CLAIM_MEMBER_SYSTEM = "http://healthec.com/id/member"
CLAIM_MEDICARE_SYSTEM = "http://healthec.com/id/medicare"
OBSERVATION_VALUE_SYSTEM = "http://unitsofmeasure.org"

ICD_CODE_SYSTEM = "http://hl7.org/fhir/sid/icd-10"
PROCEDURE_CPT_SYSTEM = "http://www.ama-assn.org/go/cpt"

VITAL_CATEGORY_CODE = "vital-signs"
VITAL_CATEGORY_DISPLAY = "Vital Signs"
VITAL_CATEGORY_SYSTEM = "http://terminology.hl7.org/CodeSystem/observation-category"
VITAL_CATEGORY_TEXT = "Vital Signs"

SMOKING_STATUS_CATEGORY_CODE = "72166-2"
SMOKING_STATUS_CATEGORY_DISPLAY = "Tobacco smoking status NHIS"
SMOKING_STATUS_CATEGORY_SYSTEM = "http://loinc.org"
SMOKING_STATUS_CATEGORY_TEXT = "Smoking Status"

ICD9_CODE_SYSTEM = "http://hl7.org/fhir/sid/icd-9-cm"
ICD10_CODE_SYSTEM = "http://hl7.org/fhir/sid/icd-10-cm"

UNKNOWN_RACE_CODE = "UNK"
UNKNOWN_RACE_DISPLAY = "unknown"
UNKNOWN_ETHNICITY_CODE = "UNK"
UNKNOWN_ETHNICITY_DISPLAY = "unknown"

GENDER_DICT = {
    "M": "male",
    "F": "female",
    "T": "other",
    "O": "other",
    "U": "unknown",
}

BOOLEAN_DICT = {
    "Y": "true",
    "T": "true",
    "N": "false",
    "F": "false",
}

MARITAL_STATUS_DICT = {
    "ANNULLED": {
        "marital_status_code": "A",
        "marital_status_display": "Annulled",
        "marital_status_system": "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus",
    },
    "DIVORCED": {
        "marital_status_code": "D",
        "marital_status_display": "Divorced",
        "marital_status_system": "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus",
    },
    "INTERLOCUTORY": {
        "marital_status_code": "I",
        "marital_status_display": "Interlocutory",
        "marital_status_system": "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus",
    },
    "LEGALLY SEPARATED": {
        "marital_status_code": "L",
        "marital_status_display": "Legally Separated",
        "marital_status_system": "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus",
    },
    "MARRIED": {
        "marital_status_code": "M",
        "marital_status_display": "Married",
        "marital_status_system": "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus",
    },
    "COMMON LAW": {
        "marital_status_code": "C",
        "marital_status_display": "Common Law",
        "marital_status_system": "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus",
    },
    "POLYGAMOUS": {
        "marital_status_code": "P",
        "marital_status_display": "Polygamous",
        "marital_status_system": "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus",
    },
    "DOMESTIC PARTNER": {
        "marital_status_code": "T",
        "marital_status_display": "Domestic Partner",
        "marital_status_system": "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus",
    },
    "UNMARRIED": {
        "marital_status_code": "U",
        "marital_status_display": "Unmarried",
        "marital_status_system": "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus",
    },
    "NEVER MARRIED": {
        "marital_status_code": "S",
        "marital_status_display": "Never Married",
        "marital_status_system": "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus",
    },
    "WIDOWED": {
        "marital_status_code": "W",
        "marital_status_display": "Widowed",
        "marital_status_system": "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus",
    },
}

# code systems
SNOMED_CODE_SYSTEM = "urn:oid:2.16.840.1.113883.6.96"
CPT_CODE_SYSTEM = "urn:oid:2.16.840.1.113883.6.12"
LONIC_CODE_SYSTEM = "urn:oid:2.16.840.1.113883.6.1"
ICT_X_CM_CODE_SYSTEM = "urn:oid:2.16.840.1.113883.6.90"
ICT_IX_CM_CODE_SYSTEM = "urn:oid:2.16.840.1.113883.6.103"
RX_NORM_CODE_SYSTEM = "urn:oid:2.16.840.1.113883.6.88"
CVX_CODE_SYSTEM = "urn:oid:2.16.840.1.113883.12.292"
NDC_CODE_SYSTEM = "urn:oid:2.16.840.1.113883.6.69"
HCPCS_CODE_SYSTEM = "urn:oid:2.16.840.1.113883.6.285"
CDT_CODE_SYSTEM = "urn:oid:2.16.840.1.113883.6.13"

# provider taxonomy
PROVIDER_SPECIALTY_DICT = {
    "207Q00000X": "Primary Care Physician",
    "207V00000X": "Obstetrics & Gynecology Physician",
    "103T00000X": "Psychologist",
    "152W00000X": "Optometrist",
    "1223S0112X": "Oral and Maxillofacial Surgery (Dentist)",
    "207RN0300X": "Nephrology Physician",
    "207L00000X": "Anesthesiology Physician",
    "363L00000X": "Nurse Practitioner",
    "363A00000X": "Physician Assistant",
    "208D00000X": "General Practice Physician",
    "333600000X": "Pharmacy",
    "282N00000X": "General Acute Care Hospital",
    "314000000X": "Skilled Nursing Facility",
    "208600000X": "Surgery Physician",
    "367500000X": "Certified Registered Nurse Anesthetist",
}

OBSERVATION_STATUS = {
    "received": "registered",
    "draft": "preliminary",
    "completed": "final",
    "amended": "amended",
    "corrected": "corrected",
    "abandoned": "cancelled",
    "error": "entered-in-error",
    "unknown": "unknown",
}

ENCOUNTER_STATUS = {
    "planned": "planned",
    "arrived": "arrived",
    "accepted": "triaged",
    "active": "in-progress",
    "suspended": "onleave",
    "completed": "finished",
    "abandoned": "cancelled",
    "error": "entered-in-error",
    "unknown": "unknown",
}
