RMSC_RESOURCE_TYPES = [
    "Patient",
    "Organization",
    "Practitioner",
    "Encounter",
    "Condition",
    "Immunization",
    "Procedure",
    "Observation",
]

# Organization Types
TYPE_PROV_CODE = "prov"
TYPE_PROV_DISPLAY = "Healthcare Provider"
TYPE_OTHER_CODE = "other"
TYPE_OTHER_DISPLAY = "Other"

LANDING_PATH_SUFIX = "landing"
PROCESSED_PATH_SUFIX = "processed"
ERROR_PATH_SUFIX = "error"

ACTIVE_STATUS = "true"

GENDER_DICT = {
    "M": "male",
    "F": "female",
    "T": "other",
    "O": "other",
    "U": "unknown",
    "0": "unknown",
    "1": "male",
    "2": "female",
}

OBSERVATION_RESULT_DICT = {
    "R": "registered",
    "P": "preliminary",
    "F": "final",
    "A": "amended",
    "C": "cancelled",
    "E": "entered in Error",
    "U": "unknown",
}

STATUS_DICT = {"1": "true", "2": "false"}

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


ENCOUNTER_CLASS_DICT = {
    "AMB": {"code": "AMB", "display": "ambulatory"},
    "EMER": {"code": "EMER", "display": "emergency"},
    "FLD": {"code": "FLD", "display": "field"},
    "HH": {"code": "HH", "display": "home health"},
    "IMP": {"code": "IMP", "display": "inpatient encounter"},
    "ACUTE": {"code": "ACUTE", "display": "inpatient acute"},
    "NONAC": {"code": "NONAC", "display": "inpatient non-acute"},
    "OBSENC": {"code": "OBSENC", "display": "observation encounter"},
    "PRENC": {"code": "PRENC", "display": "pre-admission"},
    "SS": {"code": "SS", "display": "short stay"},
    "VR": {"code": "OBSVRENC", "display": "virtual"},
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
