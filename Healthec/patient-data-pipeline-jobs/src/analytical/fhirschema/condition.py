from pyspark.sql.types import ArrayType, StringType, StructField, StructType

"""
Schema for fhir resource : schema will be based on the fhir template.
"""

# fhir json structure for condition
# condition_json = {
# "id": "dfghjkl",
#     "identifier": [
#         {
#             "type": {
#                 "coding": [
#                     {
#                         "system": "identifier_type_code_system",
#                         "code": "RI",
#                         "display": "identifier_type_code_display"
#                     }
#                 ],
#                 "text": "identifier_type_code_text"
#             },
#             "system": "identifier_system",
#             "value": "identifier_value"
#         }
#     ],
#     "clinicalStatus": {
#         "coding": [
#             {
#                 "system": "clinical_status_code_system",
#                 "code": "clinical_status_code",
#                 "display": "clinical_status_code_display"
#             }
#         ],
#         "text": "clinical_status_text"
#     },
#     "verificationStatus": {
#         "coding": [
#             {
#                 "system": "verification_status_code_system",
#                 "code": "verification_status_code",
#                 "display": "verification_status_code_display"
#             }
#         ],
#         "text": "verification_status_text"
#     },
#     "category": [
#         {
#             "coding": [
#                 {
#                     "system": "category_code_system",
#                     "code": "category_code",
#                     "display": "category_code_display"
#                 }
#             ],
#             "text": "category_text"
#         }
#     ],
#     "severity": {
#         "coding": [
#             {
#                 "system": "severity_code_system",
#                 "code": "severity_code",
#                 "display": "severity_code_display"
#             }
#         ],
#         "text": "condition_code_text"
#     },
#     "code": {
#         "coding": [
#             {
#                 "system": "condition_code_system",
#                 "code": "condition_code",
#                 "display": "condition_code_display"
#             }
#         ],
#         "text": "condition_code_text"
#     },
#     "onsetDateTime": "2014-05-15T05:57:03.903-05:00",
#     "onsetPeriod": {
#         "start": "2014-05-15T05:57:03.903-05:00",
#         "end": "2014-05-15T05:57:03.903-05:00"
#     },
#     "abatementDateTime": "2014-05-15T05:57:03.903-05:00",
#     "abatementPeriod": {
#         "start": "2014-05-15T05:57:03.903-05:00",
#         "end": "2014-05-15T05:57:03.903-05:00"
#     },
#     "bodySite": [
#         {
#             "coding": [
#                 {
#                     "system": "bodysite_code_system",
#                     "code": "bodysite_code",
#                     "display": "bodysite_code_display"
#                 }
#             ],
#             "text": "bodysite_text"
#         }
#     ],
#     "subject": {
#         "id": "ba13f91c-9e6a-4945-99ad-d813a3902953",
#         "reference": "Patient/ba13f91c-9e6a-4945-99ad-d813a3902953",
#         "type": "Patient"
#     },
#     "encounter": {
#         "id": "28fae577-1c2e-4216-9038-2be4beda2da2",
#         "reference": "Encounter/28fae577-1c2e-4216-9038-2be4beda2da2",
#         "type": "Encounter"
#     },
#     "recordedDate": "2014-05-15T05:57:03.903-05:00",
#     "participant": [
#         {
#             "function": {
#                 "coding": [
#                     {
#                         "system": "severity_code_system",
#                         "code": "severity_code",
#                         "display": "severity_code_display"
#                     }
#                 ],
#                 "text": "condition_code_text"
#             },
#             "actor": {
#                 "id": "35048967-da22-442c-910c-65c01a13c5c4",
#                 "reference": "Practitioner/35048967-da22-442c-910c-65c01a13c5c4",
#                 "type": "Practitioner"
#             }
#         }
#     ],
#     "resourceType": "Condition"
# }

# build schema
# df=spark.read.json(spark.sparkContext.parallelize([condition_json]))
# schema_json = df.schema.json()
# CONDITION_SCHEMA = StructType.fromJson(json.loads(condition_json))

# condition schema for above json
CONDITION_SCHEMA = StructType(
    [
        StructField("abatementDateTime", StringType(), True),
        StructField(
            "abatementPeriod",
            StructType(
                [
                    StructField("end", StringType(), True),
                    StructField("start", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("id", StringType(), True),
        StructField(
            "bodySite",
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
            "clinicalStatus",
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
            "code",
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
        StructField(
            "identifier",
            ArrayType(
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
                                    ),
                                    StructField("text", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                        StructField("value", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("onsetDateTime", StringType(), True),
        StructField(
            "onsetPeriod",
            StructType(
                [
                    StructField("end", StringType(), True),
                    StructField("start", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "participant",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "actor",
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
                            "function",
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
        StructField("recordedDate", StringType(), True),
        StructField("resourceType", StringType(), True),
        StructField(
            "severity",
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
        StructField(
            "verificationStatus",
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
)
