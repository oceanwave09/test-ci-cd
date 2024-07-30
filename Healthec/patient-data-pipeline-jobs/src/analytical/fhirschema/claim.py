from pyspark.sql.types import ArrayType, BooleanType, DecimalType, StringType, StructField, StructType, FloatType

"""
Schema for fhir resource : schema will be based on the fhir template.
"""

# fhir json structure
# claim_json = {
#   "resourceType": "Claim",
#   "id": "dc1e6ebe-06c7-4680-8667-96f934b8bbe0",
#   "related": [
#     {
#       "reference": {
#         "system": "system",
#         "value": 12
#       }
#     }
#   ],
#   "identifier": [
#     {
#       "type": {
#         "coding": [
#           {
#             "system": "identifier_type_code_system",
#             "code": "RI",
#             "display": "identifier_type_code_display"
#           }
#         ],
#         "text": "identifier_type_code_text"
#       },
#       "assigner": {
#         "id": "47d6a1e2-778d-4dd1-babc-04ffd2d4cda4",
#         "reference": "Organization/47d6a1e2-778d-4dd1-babc-04ffd2d4cda4",
#         "type": "Organization"
#       }
#     }
#   ],
#   "status": "active",
#   "type": {
#     "coding": [
#       {
#         "system": "type_code_system",
#         "code": "institutional",
#         "display": "type_code_display"
#       }
#     ],
#     "text": "type_text"
#   },
#   "use": "claim",
#   "patient": {
#     "id": "ba13f91c-9e6a-4945-99ad-d813a3902953",
#     "reference": "Patient/ba13f91c-9e6a-4945-99ad-d813a3902953",
#     "type": "Patient"
#   },
#   "billablePeriod": {
#     "start": "2014-05-15T05:57:03.903-05:00",
#     "end": "2014-05-15T05:57:03.903-05:00"
#   },
#   "created": "2014-05-15T05:57:03.903-05:00",
#   "enterer": {
#     "id": "35048967-da22-442c-910c-65c01a13c5c4",
#     "reference": "Practitioner/35048967-da22-442c-910c-65c01a13c5c4",
#     "type": "Practitioner"
#   },
#   "insurer": {
#     "id": "47d6a1e2-778d-4dd1-babc-04ffd2d4cda4",
#     "reference": "Organization/47d6a1e2-778d-4dd1-babc-04ffd2d4cda4",
#     "type": "Organization"
#   },
#   "provider": {
#     "id": "35048967-da22-442c-910c-65c01a13c5c4",
#     "reference": "Practitioner/35048967-da22-442c-910c-65c01a13c5c4",
#     "type": "Practitioner"
#   },
#   "priority": {
#     "coding": [
#       {
#         "system": "priority_system",
#         "code": "priority_code",
#         "display": "priority_display"
#       }
#     ],
#     "text": "priority_text"
#   },
#   "fundsReserve": {
#     "coding": [
#       {
#         "system": "funds_system",
#         "code": "funds_code",
#         "display": "funds_display"
#       }
#     ],
#     "text": "funds_text"
#   },
#   "prescription": {
#     "id": "f44a4d2b-6813-4b06-a511-fbd39a41393c",
#     "reference": "MedicationRequest/f44a4d2b-6813-4b06-a511-fbd39a41393c",
#     "type": "MedicationRequest"
#   },
#   "originalPrescription": {
#     "id": "f44a4d2b-6813-4b06-a511-fbd39a41393c",
#     "reference": "MedicationRequest/f44a4d2b-6813-4b06-a511-fbd39a41393c",
#     "type": "MedicationRequest"
#   },
#   "payee": {
#     "type": {
#       "coding": [
#         {
#           "system": "payee_type_code_system",
#           "code": "payee_type_code",
#           "display": "payee_type_code_display"
#         }
#       ],
#       "text": "payee_type_text"
#     },
#     "party": {
#       "id": "00000000-0000-0000-0000-000000000000",
#       "reference": "Organization/47d6a1e2-778d-4dd1-babc-04ffd2d4cda4"
#     }
#   },
#   "facility": {
#     "id": "29a04ab8-9ff7-4594-af8d-3b35b85209a4",
#     "reference": "Location/29a04ab8-9ff7-4594-af8d-3b35b85209a4",
#     "type": "Location"
#   },
#   "diagnosis": [
#     {
#       "diagnosisCodeableConcept": null,
#       "diagnosisReference": {
#         "id": "4bd5f33f-3bbf-4e08-b093-a88b601e71fd",
#         "reference": "Condition/4bd5f33f-3bbf-4e08-b093-a88b601e71fd",
#         "type": "Condition"
#       },
#       "sequence": 1
#     }
#   ],
#   "procedure": [
#     {
#       "procedureCodeableConcept": null,
#       "procedureReference": {
#         "id": "658b726b-1c7e-49c9-9d5d-5e72b5eda2dc",
#         "reference": "Procedure/658b726b-1c7e-49c9-9d5d-5e72b5eda2dc",
#         "type": "Procedure"
#       },
#       "sequence": 1
#     }
#   ],
#   "insurance": [
#     {
#       "sequence": 1,
#       "focal": true,
#       "coverage": {
#         "id": "1fa4606e-639a-4814-88dc-39bfc755567c",
#         "reference": "Coverage/1fa4606e-639a-4814-88dc-39bfc755567c",
#         "type": "Coverage"
#       },
#       "preAuthRef": [
#         "pre_auth_ref"
#       ]
#     }
#   ],
#   "accident": {
#     "date": "1999-11-11",
#     "type": {
#       "coding": [
#         {
#           "system": "type_code_system",
#           "code": "type_code",
#           "display": "type_code_display"
#         }
#       ],
#       "text": "type_text"
#     },
#     "locationAddress": {
#       "line": [
#         "accident_line_1",
#         "accident_line_2"
#       ],
#       "city": "accident_city",
#       "state": "accident_state",
#       "postalCode": "accident_zip"
#     },
#     "locationReference": {
#       "id": "00000000-0000-0000-0000-000000000000",
#       "reference": "Location/29a04ab8-9ff7-4594-af8d-3b35b85209a4"
#     }
#   },
#   "item": [
#     {
#       "sequence": 1,
#       "diagnosisSequence": [
#         1,
#         2,
#         3
#       ],
#       "procedureSequence": [
#         1,
#         2
#       ],
#       "revenue": {
#         "coding": [
#           {
#             "system": "revenue_code_system",
#             "code": "revenue_code",
#             "display": "revenue_code_display"
#           }
#         ],
#         "text": "revenue_text"
#       },
#       "category": {
#         "coding": [
#           {
#             "system": "category_code_system",
#             "code": "category_code",
#             "display": "category_code_display"
#           }
#         ],
#         "text": "category_text"
#       },
#       "productOrService": {
#         "coding": [
#           {
#             "system": "service_code_system",
#             "code": "service_code",
#             "display": "service_code_display"
#           }
#         ],
#         "text": "service_text"
#       },
#       "quantity": {
#         "value": 2,
#         "unit": "unit",
#         "system": "unit_code_system",
#         "code": "unit_code"
#       },
#       "unitPrice": {
#         "value": 100,
#         "currency": "USD"
#       },
#       "factor": "1",
#       "net": {
#         "value": 200,
#         "currency": "USD"
#       },
#       "encounter": [
#         {
#           "id": "28fae577-1c2e-4216-9038-2be4beda2da2",
#           "reference": "Encounter/28fae577-1c2e-4216-9038-2be4beda2da2",
#           "type": "Encounter"
#         }
#       ],
#       "servicedDate": "2023-08-15",
#       "servicedPeriod": {
#         "start": "2014-05-15T05:57:03.903-05:00",
#         "end": "2014-05-15T05:57:03.903-05:00"
#       },
#       "locationReference": {
#         "id": "29a04ab8-9ff7-4594-af8d-3b35b85209a4",
#         "reference": "Location/29a04ab8-9ff7-4594-af8d-3b35b85209a4",
#         "type": "Location"
#       }
#     }
#   ],
#   "total": {
#     "value": 200,
#     "currency": "USD"
#   }
# }

# build schema
# df=spark.read.json(spark.sparkContext.parallelize([claim_json]))
# schema_json = df.schema.json()
# CLAIM_SCHEMA = StructType.fromJson(json.loads(claim_json))

CLAIM_SCHEMA = StructType(
    [
        StructField(
            "accident",
            StructType(
                [
                    StructField("date", StringType(), True),
                    StructField(
                        "locationAddress",
                        StructType(
                            [
                                StructField("city", StringType(), True),
                                StructField("line", ArrayType(StringType(), True), True),
                                StructField("postalCode", StringType(), True),
                                StructField("state", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "locationReference",
                        StructType(
                            [
                                StructField("id", StringType(), True),
                                StructField("reference", StringType(), True),
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
                ]
            ),
            True,
        ),
        StructField(
            "billablePeriod",
            StructType(
                [
                    StructField("end", StringType(), True),
                    StructField("start", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("created", StringType(), True),
        StructField(
            "diagnosis",
            ArrayType(
                StructType(
                    [
                        StructField("diagnosisCodeableConcept", StringType(), True),
                        StructField(
                            "diagnosisReference",
                            StructType(
                                [
                                    StructField("id", StringType(), True),
                                    StructField("reference", StringType(), True),
                                    StructField("type", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                        StructField("sequence", DecimalType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            "enterer",
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
            "facility",
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
            "fundsReserve",
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
        StructField(
            "insurance",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "coverage",
                            StructType(
                                [
                                    StructField("id", StringType(), True),
                                    StructField("reference", StringType(), True),
                                    StructField("type", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                        StructField("focal", BooleanType(), True),
                        StructField("preAuthRef", ArrayType(StringType(), True), True),
                        StructField("sequence", DecimalType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            "insurer",
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
            "item",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "category",
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
                        StructField("diagnosisSequence", ArrayType(DecimalType(), True), True),
                        StructField(
                            "encounter",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("id", StringType(), True),
                                        StructField("reference", StringType(), True),
                                        StructField("type", StringType(), True),
                                    ]
                                ),
                                True,
                            ),
                            True,
                        ),
                        StructField("factor", StringType(), True),
                        StructField(
                            "locationReference",
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
                            "net",
                            StructType(
                                [
                                    StructField("currency", StringType(), True),
                                    StructField("value", FloatType(), True),
                                ]
                            ),
                            True,
                        ),
                        StructField("procedureSequence", ArrayType(DecimalType(), True), True),
                        StructField(
                            "productOrService",
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
                            "revenue",
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
                        StructField("servicedDate", StringType(), True),
                        StructField(
                            "servicedPeriod",
                            StructType(
                                [
                                    StructField("end", StringType(), True),
                                    StructField("start", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                        StructField(
                            "unitPrice",
                            StructType(
                                [
                                    StructField("currency", StringType(), True),
                                    StructField("value", FloatType(), True),
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
            "originalPrescription",
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
            "patient",
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
            "payee",
            StructType(
                [
                    StructField(
                        "party",
                        StructType(
                            [
                                StructField("id", StringType(), True),
                                StructField("reference", StringType(), True),
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
                ]
            ),
            True,
        ),
        StructField(
            "prescription",
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
            "priority",
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
            "procedure",
            ArrayType(
                StructType(
                    [
                        StructField("procedureCodeableConcept", StringType(), True),
                        StructField(
                            "procedureReference",
                            StructType(
                                [
                                    StructField("id", StringType(), True),
                                    StructField("reference", StringType(), True),
                                    StructField("type", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                        StructField("sequence", DecimalType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            "provider",
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
            "related",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "reference",
                            StructType(
                                [
                                    StructField("system", StringType(), True),
                                    StructField("value", FloatType(), True),
                                ]
                            ),
                            True,
                        )
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("resourceType", StringType(), True),
        StructField("status", StringType(), True),
        StructField(
            "total",
            StructType(
                [
                    StructField("currency", StringType(), True),
                    StructField("value", FloatType(), True),
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
    ]
)
