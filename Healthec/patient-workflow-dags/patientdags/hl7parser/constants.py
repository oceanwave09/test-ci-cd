HL7_CONFIG = {
    "ADT_A01": "schemas/ADT_A01_config.json",
    "ADT_A02": "schemas/ADT_A02_config.json",
    "ADT_A03": "schemas/ADT_A03_config.json",
    "ADT_A04": "schemas/ADT_A04_config.json",
    "ADT_A05": "schemas/ADT_A05_config.json",
    "ADT_A06": "schemas/ADT_A06_config.json",
    "ADT_A07": "schemas/ADT_A07_config.json",
    "ADT_A08": "schemas/ADT_A08_config.json",
    "ADT_A11": "schemas/ADT_A11_config.json",
    "ADT_A13": "schemas/ADT_A13_config.json",
    "ADT_A28": "schemas/ADT_A28_config.json",
    "ADT_A31": "schemas/ADT_A31_config.json",
    "ORU_R01": "schemas/ORU_R01_config.json",
    "VXU_V04": "schemas/VXU_V04_config.json",
}


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

DEFAUTL_SEQ_INDEX = 1
SEQUENCE_IDENTIFIER = "MSH"
MSG_TYPE_IDEN_INDEX = 8

RESOURCE_IDENTIFIER_SYSTEM = "http://healthec.com/id/source_id"
MRN_IDENTIFIER_SYSTEM = "http://healthec.com/id/mrn"
DRIVING_LICENSE_SYSTEM = "http://healthec.com/id/driving_license"
MBI_SYSTEM = "http://healthec.com/id/mbi"
MEDICAID_SYSTEM = "http://healthec.com/id/medicaid"
STATE_LICENSE_SYSTEM = "http://healthec.com/id/state_license"
SUBSCRIBER_SYSTEM = "http://healthec.com/id/subscriber_id"
MEMBER_SYSTEM = "http://healthec.com/id/member"
MEDICARE_SYSTEM = "http://healthec.com/id/medicare"
PATIENT_INTERNAL_SYSTEM = "http://healthec.com/id/patient_internal_id"
PATIENT_EXTERNAL_SYSTEM = "http://healthec.com/id/patient_external_id"

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

VIST_ID_SYSTEM = "http://healthec.com/id/vist_id"
PATIENT_ACCOUNT_NUMBER_SYSTEM = "http://healthec.com/id/patient_account_number"

ADDITIONAL_KEYS = ["source_file_name", "assigner_organization_id", "internal_id"]

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

# Organization Types
TYPE_PROV_CODE = "prov"
TYPE_PROV_DISPLAY = "Healthcare Provider"
TYPE_PAY_CODE = "pay"
TYPE_PAY_DISPLAY = "Payer"
TYPE_DEPT_CODE = "dept"
TYPE_DEPT_DISPLAY = "Hospital Department"
TYPE_OTHER_CODE = "other"
TYPE_OTHER_DISPLAY = "Other"

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


ENCOUNTER_PARTICIPATION_TYPE = {
    "admitter": {
        "type_code": "ADM",
        "type_system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
        "type_display": "admitter",
    },
    "attender": {
        "type_code": "ATND",
        "type_system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
        "type_display": "attender",
    },
    "consultant": {
        "type_code": "CON",
        "type_system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
        "type_display": "consultant",
    },
    "discharger": {
        "type_code": "DIS",
        "type_system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
        "type_display": "discharger",
    },
    "referrer": {
        "type_code": "REF",
        "type_system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
        "type_display": "referrer",
    },
}

OBSERVATION_STATUS = {
    "R": "registered",
    "S": "registered",
    "I": "registered",
    "P": "preliminary",
    "F": "final",
    "U": "amended",
    "C": "corrected",
    "D": "cancelled",
    "W": "entered-in-error",
    "X": "unknown",
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

ALLERY_CRITICALITY = {"M": "low", "S": "high"}

ALLERGY_TYPE = ["allergy", "intolerance"]

COVERAGE_STATUS_CODES = ["active", "cancelled", "draft", "entered-in-error"]

ENCOUNTER_PRACTITIONER_TYPE = {
    "attending_doctor": {
        "type_code": "ATND",
        "type_display": "attender",
        "type_system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
    },
    "referring_doctor": {
        "type_code": "REF",
        "type_display": "referrer",
        "type_system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
    },
    "consulting_doctor": {
        "type_code": "CON",
        "type_display": "consultant",
        "type_system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
    },
    "admitting_doctor": {
        "type_code": "ADM",
        "type_display": "admitter",
        "type_system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
    },
}

PROCEDURE_PRACTITIONER_TYPE = {
    "anesthesiologist": {"code": "88189002", "display": "Anesthesiologist"},
    "surgeon": {"code": "304292004", "display": "Surgeon"},
    "procedure_practitioner": {"code": "224936003 ", "display": "General practitioner locum"},
}

TRANSFORM_PRACTICE_TYPES = {
    "PD1": ["patient_primary_organization"],
    "IN1": ["insurance_company", "insured_group_emp_organization"],
    "OBX": ["performing_organization"],
    "MSH": ["sending_facility"],
    "RXA": ["substance_manufacturer_name"],
}

TRANSFORM_PRACTITIONER_TYPES = {
    "PD1": ["patient_primary_care_provider"],
    "OBX": ["responsible_observer"],
    "PV1": ["attending_doctor", "referring_doctor", "consulting_doctor", "admitting_doctor"],
    "PR1": ["anesthesiologist", "surgeon", "procedure_practitioner"],
    "RXA": ["administering_provider"],
    "ORC": ["ordering_provider"],
}

TRANSFORM_LOCATION_TYPES = {"PV1": ["assigned_patient"]}

ALLERGY_CATEGORY = ["food", "medication", "environment", "biologic"]

AVAILABLE_SEGMENTS = [
    "MSH",
    "PID",
    "PD1",
    "NK1",
    "PV1",
    "PV2",
    "PR1",
    "OBX",
    "DG1",
    "AL1",
    "IN1",
    "IN2",
    "ORC",
    "RXR",
    "RXA",
]

IMMUNIZATION_STATUS = {"CP": "completed", "ER": "entered-in-error", "NA": "not-done"}

SERVICE_REQUEST_STATUS = {
    "CA": "revoke",
    "A": "draft",
    "CM": "completed",
    "DC": "on-hold",
    "ER": "entered-in-error",
    "HD": "on-hold",
    "IP": "active",
    "RP": "revoke",
    "SC": "unknown",
}

SEGMENT_SCHEMA = "schemas/{}_segment.json"
SCHEMAS = {
    "ADT_XXX": {
        "MSH": {},
        "SFT": [],
        "EVN": {},
        "PID": {},
        "PD1": {},
        "ROL": [],
        "NK1": [],
        "PV1": {},
        "PV2": {},
        "DB1": [],
        "OBX": [],
        "AL1": [],
        "DG1": [],
        "DRG": {},
        "procedure": [{"PR1": {}, "ROL": []}],
        "GT1": [],
        "insurance": [{"IN1": {}, "IN2": {}, "IN3": [], "ROL": []}],
        "ACC": {},
        "UB1": {},
        "UB2": {},
        "PDA": {},
    },
    "ORU_R01": {
        "MSH": {},
        "SFT": [],
        "patient_result": [
            {
                "patient": {"PID": {}, "PD1": {}, "NTE": [], "NK1": [], "visit": {"PV1": {}, "PV2": {}}},
                "order_observation": [
                    {
                        "ORC": {},
                        "OBR": {},
                        "NTE": [],
                        "timing": [{"TQ1": {}, "TQ2": []}],
                        "CTD": {},
                        "observation": [{"OBX": {}, "NTE": []}],
                        "FT1": [],
                        "CTI": [],
                        "specimen": [{"SPM": {}, "OBX": []}],
                    }
                ],
            }
        ],
        "DSC": {},
    },
    "VXU_V04": {
        "MSH": {},
        "SFT": [],
        "PID": {},
        "PD1": {},
        "NK1": [],
        "patient": {"PV1": {}, "PV2": {}},
        "GT1": [],
        "insurance": [{"IN1": {}, "IN2": {}, "IN3": {}}],
        "order": [
            {
                "ORC": {},
                "timing": [{"TQ1": {}, "TQ2": []}],
                "RXA": {},
                "RXR": {},
                "observation": [{"OBX": {}, "NTE": []}],
            }
        ],
    },
}
