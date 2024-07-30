# General Constants
S3_SCHEMA = "s3a"
FIELD_SEPARATOR = ","

# FHIR BUNDLE TEMP PATH
BUNDLE_TEMP_PATH = "/tmp"

# Validation prefix
GX_DATA_DOCS_DIR = "airflow-workflow/great-expectations/data_docs"
GX_VAL_DIR = "airflow-workflow/great-expectations/validations"

# Database Protocols
POSTGRES = "postgres"
MSSQL = "mssql"

# JDBC Driver Class
POSTGRES_DRIVER = "org.postgresql.Driver"
MSSQL_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

# Kafka Event
EVENT_COMPLETE = "validation_complete"
EVENT_START = "validation_start"
EVENT_SUCCESS_STATUS = "success"
EVENT_FAILURE_STATUS = "failure"

# Acceptable return codes from domain services
RESPONSE_CODE_200 = 200
RESPONSE_CODE_201 = 201
RESPONSE_CODE_409 = 409
ACCEPTABLE_RETURN_CODES = [
    RESPONSE_CODE_200,
    RESPONSE_CODE_201,
    RESPONSE_CODE_409,
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

# Default Values
DEFAULT_USER_NAME = "data-pipeline"
INGESTION_PIPELINE_USER = "ingestion-pipeline"
ANALYTICAL_PIPELINE_USER = "analytical-pipeline"


# Organization Types
TYPE_PROV_CODE = "prov"
TYPE_PROV_DISPLAY = "Healthcare Provider"
TYPE_DEPT_CODE = "dept"
TYPE_DEPT_DISPLAY = "Hospital Department"

# Constants used in FHIR bundle creation
LOCATION_MODE = "kind"
ACTIVE_STATUS = "true"
STATUS_ACTIVE = "active"
STATUS_INACTIVE = "inactive"

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

PATIENT_MRN_SYSTEM = "http://healthec.com/identifier/patient/mrn"
ICD9_CODE_SYSTEM = "http://hl7.org/fhir/sid/icd-9-cm"
ICD10_CODE_SYSTEM = "http://hl7.org/fhir/sid/icd-10-cm"

UNKNOWN_RACE_CODE = "UNK"
UNKNOWN_RACE_DISPLAY = "unknown"
UNKNOWN_ETHNICITY_CODE = "UNK"
UNKNOWN_ETHNICITY_DISPLAY = "unknown"

COMMA_SYMBOL = ","

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

RACE_DICT = {
    "0": "unknown",
    "1": "white",
    "2": "black",
    "3": "other",
    "4": "asian",
    "5": "hispanic",
    "6": "north American Native",
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

# NCQA Source Systems
NCQA_MEMBER_ID_SRC_SYSTEM = "http://healthec.com/ncqa/member_id"
NCQA_SOURCE_FILE_SYSTEM = "https://healthec.com/identifiers/file"
NCQA_PROVIDER_ID_SRC_SYSTEM = "http://healthec.com/ncqa/provider_id"
NCQA_CLAIM_ID_SRC_SYSTEM = "http://healthec.com/ncqa/claim_id"
HOSPICE_URL = "http://healthec.com/extensions/patients/flags/hospice"
LONG_TERM_INSTITUTE_CARE_URL = "http://healthec.com/extensions/patients/flags/long-term-institutional-care"
RUN_DATE_URL = "http://healthec.com/extensions/patients/flags/run-date"

# Custom Source System
VACCINE_MANUFACTURER_URL = "http://healthec.com/extensions/manufacturer-organization"

# Claim Response
ADJUDICATION_CATEGORY = {
    "submitted": {
        "category_code": "submitted",
        "category_display": "Submitted Amount",
        "category_system": "http://terminology.hl7.org/CodeSystem/adjudication",
    },
    "copay": {
        "category_code": "copay",
        "category_display": "CoPay",
        "category_system": "http://terminology.hl7.org/CodeSystem/adjudication",
    },
    "eligible": {
        "category_code": "eligible",
        "category_display": "Eligible Amount",
        "category_system": "http://terminology.hl7.org/CodeSystem/adjudication",
    },
    "deductible": {
        "category_code": "deductible",
        "category_display": "Deductible",
        "category_system": "http://terminology.hl7.org/CodeSystem/adjudication",
    },
    "unallocdeduct": {
        "category_code": "unallocdeduct",
        "category_display": "Unallocated Deductible",
        "category_system": "http://terminology.hl7.org/CodeSystem/adjudication",
    },
    "eligpercent": {
        "category_code": "eligpercent",
        "category_display": "Eligible %",
        "category_system": "http://terminology.hl7.org/CodeSystem/adjudication",
    },
    "tax": {
        "category_code": "tax",
        "category_display": "Tax",
        "category_system": "http://terminology.hl7.org/CodeSystem/adjudication",
    },
    "benefit": {
        "category_code": "benefit",
        "category_display": "Benefit Amount",
        "category_system": "http://terminology.hl7.org/CodeSystem/adjudication",
    },
    # "coinsurance": {
    #     "category_code": "coinsurance",
    #     "category_display": "Co-Insurance",
    #     "category_system": "http://terminology.hl7.org/CodeSystem/adjudication",
    # },
    # "allowed": {
    #     "category_code": "allowed",
    #     "category_display": "Allowed Amount",
    #     "category_system": "http://terminology.hl7.org/CodeSystem/adjudication",
    # },
    # "discount": {
    #     "category_code": "discount",
    #     "category_display": "Discount Amount",
    #     "category_system": "http://terminology.hl7.org/CodeSystem/adjudication",
    # },
    # "patient": {
    #     "category_code": "patient-paid",
    #     "category_display": "Patient Paid Amount",
    #     "category_system": "http://terminology.hl7.org/CodeSystem/adjudication",
    # },
    # "otherpayer": {
    #     "category_code": "other-payer-paid",
    #     "category_display": "Other Payer Paid Amount",
    #     "category_system": "http://terminology.hl7.org/CodeSystem/adjudication",
    # },
    # "medicare": {
    #     "category_code": "medicare-paid",
    #     "category_display": "Medicare Paid Amount",
    #     "category_system": "http://terminology.hl7.org/CodeSystem/adjudication",
    # },
}

# ENCOUNTER_STATUS
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

# supplemental data
SUPPLEMENTAL_DATA_URL = "http://healthec.com/extensions/patients/flags/supplemental-data"
SUPPLEMENTAL_DATA_VALUE_BOOLEAN = "true"


# Radaid batch type
RADAID_BATCH_TYPE_DICT = {
    "1": "Patient",
    "2": "Pt Outreach",
    "3": "Breast Scr",
    "4": "Breast Diag",
    "5": "Breast Proc",
    "6": "Breast Tx",
    "7": "Cervical Scr",
    "8": "Cervical Diag",
    "9": "Cervical Proc",
    "10": "Cervical Tx",
}

# Radaid race
RADAID_RACE_DICT = {
    "1": "White/Caucasian",
    "2": "Black",
    "3": "American Indian or Alaskan Native",
    "4": "Asian American",
    "5": "Native Hawaiian or Other Pacific Islander",
    "6": "More than one race",
    "7": "Other",
    "88": "Prefer not to answer",
    "99": "Missing",
}

# Radaid ethnicity
RADAID_EHNICITY_DICT = {
    "1": "Latina/Hispanic/Latinx",
    "2": "Not Latina/Hispanic/Latinx",
    "3": "Prefer not to answer",
    "99": "Missing",
}

# Radaid language
RADAID_LANGUAGE_DICT = {
    "1": "English only",
    "2": "Spanish only",
    "3": "No Preference",
    "4": "Other",
    "99": "Missing",
}

# Radaid marital status
RADAID_MARITAL_STATUS_DICT = {
    "1": "Single",
    "2": "Married or living with a partner",
    "3": "Divorced/Separated",
    "4": "Widowed",
    "88": "Prefer not to answer",
    "99": "Missing",
}

# Radaid pathology results
PATHOLOGY_RESULT_DICT = {
    "1": "DCIS",
    "2": "IDS",
    "3": "ILC",
    "4": "Metastatic lymph node",
    "5": "Other Cancer",
    "99": "Missing",
}

# Radaid tnm stage
TNM_STAGE_DICT = {
    "0": "Stage 0",
    "1": "Stage I",
    "2": "Stage II",
    "3": "Stage III",
    "4": "Stage IV",
    "5": "Unstaged",
    "99": "Missing",
}

# Radaid treatments
TREATMENT_DICT = {
    "Surgery. Lumpectomy with/without axillary node removal": "1",
    "Mastectomy (single) with/without axillary node removal": "2",
    "Bilateral mastectomy with/without axillary node removal": "3",
    "Oophorectomy": "4",
    "Chemotherapy": "5",
    "Hormonal therapy (e.g. Tamoxifen, Anastrazole, Aromatase inhibitors, SERMS)": "6",
    "Targeted drugs (e.g. trastuzumab, pertuzumab, or abemaciclib)": "7",
    "Immunotherapy": "8",
    "Radiotherapy": "9",
    "Missing": "99",
}

# Radaid cxca pathology results
CXCA_PATHOLOGY_RESULT_DICT = {
    "1": "Normal",
    "2": "CIN I",
    "3": "CIN 2",
    "4": "CIN 3/AIS",
    "5": "Cervical Cancer",
    "99": "Missing",
}

# Radaid cxca screreening methods
CXCA_SCR_DICT = {"Pap Testing": "1", "HPV": "2", "Co-Testing": "3", "Missing/Not Recorded": "99"}

# Radaid cxca screreening results
HPV_PAP_RESULT_DICT = {"1": "Negative", "2": "Positive", "99": "Missing"}

# Radaid brca procedure names
PROCEDURE_NAME_DICT = {
    "Unilateral Mammogram": "1",
    "Bilateral Mammogram": "2",
    "Unilateral Breast Ultrasound": "3",
    "Bilateral Breast Ultrasound": "4",
    "Stereotactic Biopsy": "5",
    "Ultrasound Guided Biopsy": "6",
    "Missing": "99",
}

# Radaid procedure code system
BRCA_PROCEDURE_CODE_SYSTEM = "https://rad-aid.org/fhir/valueset/breast-procedure-name"

# Radaid diag outcome code system
BRCA_DIAG_OUTCOME_CODE_SYSTEM = "https://rad-aid.org/fhir/valueset/bi-rads"

# Radaid proc outcome code system
BRCA_PROC_OUTCOME_CODE_SYSTEM = "https://rad-aid.org/fhir/valueset/breast-pathology-result"

# Radaid treatment code system
BRCA_TREATMENT_CODE_SYSTEM = "https://rad-aid.org/fhir/valueset/breast-treatment"

# Radaid tx outcome code system
TX_OUTCOME_CODE_SYSTEM = "https://rad-aid.org/fhir/valueset/tnm-stage"

# Radaid diag code system
CXCA_DIAG_CODE_SYSTEM = "https://rad-aid.org/fhir/valueset/cervical-diagnostic-method"

# Radaid outcome code system
CXCA_DIAG_OUTCOME_CODE_SYSTEM = "https://rad-aid.org/fhir/valueset/cervical-pathology-result"

# Radaid proc code system
CXCA_PROCEDURE_CODE_SYSTEM = "https://rad-aid.org/fhir/valueset/cervical-patient-management-procedure"

# Radaid screening code system
CXCA_SCREENING_CODE_SYSTEM = "https://rad-aid.org/fhir/valueset/cervical-screening-method"

# Radaid screening code system
CXCA_SCR_OUTCOME_CODE_SYSTEM = "https://rad-aid.org/fhir/valueset/cervical-screening-result"

# Radaid language code system
RADAID_LANGUAGE_CODE_SYSTEM = "https://rad-aid.org/fhir/valueset/language"

# Radaid marital status code system
RADAID_MARITAL_CODE_SYSTEM = "https://rad-aid.org/fhir/valueset/marital-status"

# Radaid BI-RADS result
BI_RADS_RESULT_DICT = {
    "0": "BI-RADS category 0",
    "1": "BI-RADS category 1",
    "2": "BI-RADS category 2",
    "3": "BI-RADS category 3",
    "4": "BI-RADS category 4",
    "5": "BI-RADS category 5",
    "6": "BI-RADS category 6",
    "99": "Missing",
}

# Radaid cervical diagnosis methods
CXCA_DIAGNOSIS_METHOD_DICT = {
    "Colposcopy": "1",
    "Biopsy": "2",
    "Imaging tests (e.g., X-Ray, Ultrasound, MRI, PET-CT Scan)": "3",
    "Lab tests": "4",
    "Visual examination (e.g., cystoscopy, sigmoidoscopy)": "5",
    "Other": "6",
    "Missing/not recorded": "99",
}

# Radaid cervical procedure names
CXCA_PROCEDURE_NAME_DICT = {
    "Continue routine screening": "1",
    "Repeat screening in ~2-4 months (e.g., Unsatisfactory PAP)": "2",
    "Repeat Screening test in 1 year": "3",
    "Repeat diagnostic test in 1 year": "4",
    "Repeat co-testing in 1 year (e.g., young high-risk patient with mild abnormality)": "5",
    "Repeat diagnostics test in 1 year": "6",
    "Diagnostic test (e.g., Colposcopy, biopsy)": "7",
    "Excisional treatment (Loop Excision)": "8",
    "Hysterectomy (cancer treatment only)": "9",
    "Other surgery/chemotherapy/radiation (cancer treatment only)": "10",
    "Missing/not recorded": "99",
}
