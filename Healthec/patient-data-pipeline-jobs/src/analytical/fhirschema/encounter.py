from pyspark.sql.types import ArrayType, StringType, StructField, StructType

"""
Schema for fhir resource : schema will be based on the fhir template.
"""
# # fhir json structure
# encounter_json = {
#   "resourceType": "Encounter",
#   "id": "encounter_fhir_id",
#   "identifier": [
#     {
#       "type": {
#         "coding": [
#           {
#             "system": "identifier_type_code_system",
#             "code": "identifier_type_code",
#             "display": "identifier_type_code_display"
#           }
#         ],
#         "text": "identifier_type_code_text"
#       },
#       "use": "identifier_use",
#       "system": "identifier_system",
#       "value": "identifier_value",
#       "assigner": {
#         "reference": "Organization/identifier_assigner_fhir_id",
#         "id": "identifier_assigner_fhir_id",
#         "type": "Organization"
#       }
#     }
#   ],
#   "status": "encounter_status",
#   "class": {
#     "system": "encounter_class_system",
#     "code": "encounter_class_code",
#     "display": "encounter_class_display"
#   },
#   "type": [
#     {
#       "coding": [
#         {
#           "system": "encounter_type_code_system",
#           "code": "encounter_type_code",
#           "display": "encounter_type_code_display"
#         }
#       ],
#       "text": "encounter_type_text"
#     }
#   ],
#   "subject": {
#     "reference": "Patient/patient_fhir_id",
#     "id": "patient_fhir_id",
#     "type": "Patient"
#   },
#   "participant": [
#     {
#       "type": [
#         {
#           "coding": [
#             {
#               "system": "participant_type_code_system",
#               "code": "participant_type_code",
#               "display": "participant_type_code_display"
#             }
#           ],
#           "text": "participant_type_text"
#         }
#       ],
#       "individual": {
#         "reference": "Practitioner/participant_fhir_id",
#         "id": "participant_fhir_id",
#         "type": "Practitioner"
#       }
#     }
#   ],
#   "period": {
#     "start": "period_start_ts",
#     "end": "period_end_ts"
#   },
#   "reasonCode": [
#     {
#       "coding": [
#         {
#           "system": "reason_code_system",
#           "code": "reason_code",
#           "display": "reason_code_display"
#         }
#       ],
#       "text": "reason_text"
#     }
#   ],
#   "reasonReference": [
#     {
#       "reference": "Condition/reason_fhir_id",
#       "id": "reason_fhir_id",
#       "type": "Condition"
#     }
#   ],
#   "diagnosis": [
#     {
#       "condition": {
#         "reference": "Condition/reason_fhir_id",
#         "id": "reason_fhir_id",
#         "type": "Condition"
#       },
#       "use": {
#         "coding": [
#           {
#             "system": "condition_use_code_system",
#             "code": "condition_use_code",
#             "display": "condition_use_code_display"
#           }
#         ],
#         "text": "condition_use_text"
#       }
#     }
#   ],
#   "hospitalization": {
#     "origin": {
#       "reference": "Organization/hospitalization_origin_fhir_id",
#       "id": "hospitalization_origin_fhir_id",
#       "type": "Organization"
#     },
#     "destination": {
#       "reference": "Organization/hospitalization_destination_fhir_id",
#       "id": "hospitalization_destination_fhir_id",
#       "type": "Organization"
#     },
#     "admitSource": {
#       "coding": [
#         {
#           "system": "admit_source_code_system",
#           "code": "admit_source_code",
#           "display": "admit_source_code_display"
#         }
#       ],
#       "text": "admit_source_text"
#     },
#     "dischargeDisposition": {
#       "coding": [
#         {
#           "system": "discharge_disposition_code_system",
#           "code": "discharge_disposition_code",
#           "display": "discharge_disposition_code_display"
#         }
#       ],
#       "text": "discharge_disposition_text"
#     }
#   },
#   "location": [
#     {
#       "location": {
#         "reference": "Location/location_fhir_id",
#         "id": "location_fhir_id",
#         "type": "Location"
#       }
#     }
#   ]
# }

# # build schema
# df=spark.read.json(spark.sparkContext.parallelize([encounter_json]))
# schema_json = df.schema.json()
# ENCOUNTER_SCHEMA = StructType.fromJson(json.loads(encounter_json))

ENCOUNTER_SCHEMA = StructType(
    [
        StructField(
            "class",
            StructType(
                [
                    StructField("code", StringType(), True),
                    StructField("display", StringType(), True),
                    StructField("system", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "diagnosis",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "condition",
                            StructType(
                                [
                                    StructField("id", StringType(), True),
                                    StructField("reference", StringType(), True),
                                    StructField("type", StringType(), True),
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
        StructField(
            "hospitalization",
            StructType(
                [
                    StructField(
                        "admitSource",
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
                        "destination",
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
                        "dietPreference",
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
                        "dischargeDisposition",
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
                        "origin",
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
                        "preAdmissionIdentifier",
                        StructType(
                            [
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
                                            )
                                        ]
                                    ),
                                    True,
                                ),
                                StructField("value", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "reAdmission",
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
                        "specialCourtesy",
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
            "length",
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
        StructField(
            "location",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "location",
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
                            "physicalType",
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
            True,
        ),
        StructField(
            "participant",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "individual",
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
                            "type",
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
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            "period",
            StructType([StructField("end", StringType(), True), StructField("start", StringType(), True)]),
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
        StructField("resourceType", StringType(), True),
        StructField(
            "serviceProvider",
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
            "serviceType",
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
        StructField("status", StringType(), True),
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
        StructField(
            "type",
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
    ]
)
