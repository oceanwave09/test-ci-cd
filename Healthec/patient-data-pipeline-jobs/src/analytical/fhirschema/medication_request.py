from pyspark.sql.types import ArrayType, StringType, StructField, StructType, DecimalType, FloatType

"""
Schema for fhir resource : schema will be based on the fhir template.
"""
# fhir json structure
# medication_request_json = {
#     "resourceType": "MedicationRequest",
#     "id": "medication_request_fhir_id",
#     "identifier": [
#         {
#             "type": {
#                 "coding": [
#                     {
#                         "system": "identifier_type_code_system",
#                         "code": "identifier_type_code",
#                         "display": "identifier_type_code_display"
#                     }
#                 ],
#                 "text": "identifier_type_code_text"
#             },
#             "use": "identifier_use",
#             "system": "identifier_system",
#             "value": "identifier_value",
#             "assigner": {
#                 "reference": "Organization/identifier_assigner_fhir_id",
#                 "id": "identifier_assigner_fhir_id",
#                 "type": "Organization"
#             }
#         }
#     ],
#     "status": "active",
#     "statusReason": {
#         "coding": [
#             {
#                 "system": "http://terminology.hl7.org/CodeSystem/medicationrequest-status-reason",
#                 "code": "altchoice",
#                 "display": "Try another treatment first"
#             }
#         ]
#     },
#     "intent": "request_intent",
#     "category": [{
#         "coding": [
#             {
#                 "system": "category_code_system",
#                 "code": "category_code",
#                 "display": "category_code_display"
#             }
#         ],
#         "text": "category_text"
#     }],
#     "priority":"enterdate",
#     "medicationCodeableConcept": {
#         "coding": [
#             {
#                 "system": "http://snomed.info/sct",
#                 "code": "medication_code",
#                 "display": "medication"
#             }
#         ]
#     },
#     "medicationReference": {
#         "reference": "Medication/medication_fhir_id",
#         "id": "medication_fhir_id",
#         "type": "Medication"
#     },
#     "subject": {
#         "reference": "Patient/patient_fhir_id",
#         "id": "patient_fhir_id",
#         "type": "Patient"
#     },
#     "encounter": {
#         "reference": "Encounter/encounter_fhir_id",
#         "id": "encounter_fhir_id",
#         "type": "Encounter"
#     },
#     "authoredOn": "2014-05-15T05:57:03.903-05:00",
#     "requester": {
#         "reference": "Practitioner/practitioner_fhir_id",
#         "id": "practitioner_fhir_id",
#         "type": "Practitioner"
#     },
#     "reasonCode": [
#         {
#             "coding": [
#                 {
#                     "system": "http://snomed.info/sct",
#                     "code": "297217002",
#                     "display": "Rib Pain (finding)"
#                 }
#             ]
#         }
#     ],
#     "note": "note",
#     "dispenseRequest": {
#         "validityPeriod": {
#             "start": "2015-01-15",
#             "end": "2016-01-15"
#         },
#         "numberOfRepeatsAllowed": 3,
#         "quantity": {
#             "value": 10,
#             "unit": "ml",
#             "system": "http://unitsofmeasure.org",
#             "code": "ml"
#         },
#         "expectedSupplyDuration": {
#             "value": 10,
#             "unit": "days",
#             "system": "http://unitsofmeasure.org",
#             "code": "d"
#         },
#         "dispenser": {
#             "reference": "Organization/id",
#             "id": "organization_fhir_id",
#             "type": "Organization"
#         }
#     },
#     "dosageInstruction": [
#         {
#             "sequence": 1,
#             "text": "one to two tablets every 4-6 hours as needed for rib pain",
#             "additionalInstruction": [
#                 {
#                     "coding": [
#                         {
#                             "system": "http://snomed.info/sct",
#                             "code": "418914006",
#                             "display": "Warning. May cause drowsiness"
#                         }
#                     ],
#                     "text": "Warning. May cause drowsiness."
#                 }
#             ],
#             "patientInstruction": "Take one to two tablets every four to six hours as needed for rib pain",
#             "timing": {
#                 "repeat": {
#                     "frequency": 1,
#                     "duration": 4,
#                     "periodUnit": "h"
#                 }
#             },
#             "route": {
#                 "coding": [
#                     {
#                         "system": "http://snomed.info/sct",
#                         "code": "26643006",
#                         "display": "Oral Route"
#                     }
#                 ],
#                 "text": "Oral Route"
#             },
#             "site": {
#                 "coding": [
#                     {
#                         "system": "http://snomed.info/sct",
#                         "code": "26643006",
#                         "display": "Oral Route"
#                     }
#                 ],
#                 "text": "Oral Route"
#             },
#             "doseAndRate": [
#                 {
#                     "type": {
#                         "coding": [
#                             {
#                                 "system": "http://terminology.hl7.org/CodeSystem/dose-rate-type",
#                                 "code": "ordered",
#                                 "display": "Ordered"
#                             }
#                         ],
#                         "text": "Orederd"
#                     },
#                     "doseQuantity": {
#                         "value": 22.22,
#                         "unit": "unit"
#                     },
#                     "rateQuantity": {
#                         "value": 67.8,
#                         "unit": "rate_unit"
#                     },
#                     "rateRange": {
#                         "low": {
#                             "value": 1
#                         },
#                         "high": {
#                             "value": 2
#                         }
#                     }
#                 }
#             ]
#         }
#     ]
# }
# build schema
# df=spark.read.json(spark.sparkContext.parallelize([medication_request_json]))
# schema_json = df.schema.json()
# MEDICATION_REQUEST_SCHEMA = StructType.fromJson(json.loads(medication_request_json))

MEDICATION_REQUEST_SCHEMA = StructType(
    [
        StructField("authoredOn", StringType(), True),
        StructField(
            "category",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "coding",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("code", StringType(), True),
                                        StructField("display", StringType(), True),
                                        StructField("system", StringType(), True),
                                    ]
                                ),
                                True,
                            ),
                            True,
                        ),
                        StructField("text", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            "dispenseRequest",
            StructType(
                [
                    StructField(
                        "dispenser",
                        StructType(
                            [
                                StructField("id", StringType(), True),
                                StructField("reference", StringType(), True),
                                StructField("type", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "expectedSupplyDuration",
                        StructType(
                            [
                                StructField("code", StringType(), True),
                                StructField("system", StringType(), True),
                                StructField("unit", StringType(), True),
                                StructField("value", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField("numberOfRepeatsAllowed", DecimalType(), True),
                    StructField(
                        "quantity",
                        StructType(
                            [
                                StructField("code", StringType(), True),
                                StructField("system", StringType(), True),
                                StructField("unit", StringType(), True),
                                StructField("value", FloatType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "validityPeriod",
                        StructType(
                            [
                                StructField("end", StringType(), True),
                                StructField("start", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
        StructField(
            "dosageInstruction",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "additionalInstruction",
                            ArrayType(
                                StructType(
                                    [
                                        StructField(
                                            "coding",
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField("code", StringType(), True),
                                                        StructField(
                                                            "display",
                                                            StringType(),
                                                            True,
                                                        ),
                                                        StructField("system", StringType(), True),
                                                    ]
                                                ),
                                                True,
                                            ),
                                            True,
                                        ),
                                        StructField("text", StringType(), True),
                                    ]
                                ),
                                True,
                            ),
                            True,
                        ),
                        StructField(
                            "doseAndRate",
                            ArrayType(
                                StructType(
                                    [
                                        StructField(
                                            "doseQuantity",
                                            StructType(
                                                [
                                                    StructField("unit", StringType(), True),
                                                    StructField("value", FloatType(), True),
                                                ]
                                            ),
                                            True,
                                        ),
                                        StructField(
                                            "rateQuantity",
                                            StructType(
                                                [
                                                    StructField("unit", StringType(), True),
                                                    StructField("value", FloatType(), True),
                                                ]
                                            ),
                                            True,
                                        ),
                                        StructField(
                                            "rateRange",
                                            StructType(
                                                [
                                                    StructField(
                                                        "high",
                                                        StructType(
                                                            [
                                                                StructField(
                                                                    "value",
                                                                    FloatType(),
                                                                    True,
                                                                )
                                                            ]
                                                        ),
                                                        True,
                                                    ),
                                                    StructField(
                                                        "low",
                                                        StructType(
                                                            [
                                                                StructField(
                                                                    "value",
                                                                    FloatType(),
                                                                    True,
                                                                )
                                                            ]
                                                        ),
                                                        True,
                                                    ),
                                                ]
                                            ),
                                            True,
                                        ),
                                        StructField(
                                            "type",
                                            StructType(
                                                [
                                                    StructField(
                                                        "coding",
                                                        ArrayType(
                                                            StructType(
                                                                [
                                                                    StructField(
                                                                        "code",
                                                                        StringType(),
                                                                        True,
                                                                    ),
                                                                    StructField(
                                                                        "display",
                                                                        StringType(),
                                                                        True,
                                                                    ),
                                                                    StructField(
                                                                        "system",
                                                                        StringType(),
                                                                        True,
                                                                    ),
                                                                ]
                                                            ),
                                                            True,
                                                        ),
                                                        True,
                                                    ),
                                                    StructField("text", StringType(), True),
                                                ]
                                            ),
                                            True,
                                        ),
                                    ]
                                ),
                                True,
                            ),
                            True,
                        ),
                        StructField("patientInstruction", StringType(), True),
                        StructField(
                            "route",
                            StructType(
                                [
                                    StructField(
                                        "coding",
                                        ArrayType(
                                            StructType(
                                                [
                                                    StructField("code", StringType(), True),
                                                    StructField("display", StringType(), True),
                                                    StructField("system", StringType(), True),
                                                ]
                                            ),
                                            True,
                                        ),
                                        True,
                                    ),
                                    StructField("text", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                        StructField("sequence", DecimalType(), True),
                        StructField(
                            "site",
                            StructType(
                                [
                                    StructField(
                                        "coding",
                                        ArrayType(
                                            StructType(
                                                [
                                                    StructField("code", StringType(), True),
                                                    StructField("display", StringType(), True),
                                                    StructField("system", StringType(), True),
                                                ]
                                            ),
                                            True,
                                        ),
                                        True,
                                    ),
                                    StructField("text", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                        StructField("text", StringType(), True),
                        StructField(
                            "timing",
                            StructType(
                                [
                                    StructField(
                                        "repeat",
                                        StructType(
                                            [
                                                StructField("duration", DecimalType(), True),
                                                StructField("frequency", DecimalType(), True),
                                                StructField("periodUnit", StringType(), True),
                                            ]
                                        ),
                                        True,
                                    )
                                ]
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            "encounter",
            StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("reference", StringType(), True),
                    StructField("type", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("id", StringType(), True),
        StructField(
            "identifier",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "assigner",
                            StructType(
                                [
                                    StructField("id", StringType(), True),
                                    StructField("reference", StringType(), True),
                                    StructField("type", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                        StructField("system", StringType(), True),
                        StructField(
                            "type",
                            StructType(
                                [
                                    StructField(
                                        "coding",
                                        ArrayType(
                                            StructType(
                                                [
                                                    StructField("code", StringType(), True),
                                                    StructField("display", StringType(), True),
                                                    StructField("system", StringType(), True),
                                                ]
                                            ),
                                            True,
                                        ),
                                        True,
                                    ),
                                    StructField("text", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                        StructField("use", StringType(), True),
                        StructField("value", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("intent", StringType(), True),
        StructField(
            "medicationCodeableConcept",
            StructType(
                [
                    StructField(
                        "coding",
                        ArrayType(
                            StructType(
                                [
                                    StructField("code", StringType(), True),
                                    StructField("display", StringType(), True),
                                    StructField("system", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                        True,
                    ),
                    StructField("text", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "medicationReference",
            StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("reference", StringType(), True),
                    StructField("type", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("priority", StringType(), True),
        StructField(
            "reasonCode",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "coding",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("code", StringType(), True),
                                        StructField("display", StringType(), True),
                                        StructField("system", StringType(), True),
                                    ]
                                ),
                                True,
                            ),
                            True,
                        ),
                        StructField("text", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            "requester",
            StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("reference", StringType(), True),
                    StructField("type", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("resourceType", StringType(), True),
        StructField("status", StringType(), True),
        StructField(
            "statusReason",
            StructType(
                [
                    StructField(
                        "coding",
                        ArrayType(
                            StructType(
                                [
                                    StructField("code", StringType(), True),
                                    StructField("display", StringType(), True),
                                    StructField("system", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                        True,
                    ),
                    StructField("text", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "subject",
            StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("reference", StringType(), True),
                    StructField("type", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)
