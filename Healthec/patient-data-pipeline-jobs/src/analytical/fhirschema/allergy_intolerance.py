from pyspark.sql.types import ArrayType, StringType, StructField, StructType

"""
Schema for fhir resource : schema will be based on the fhir template.
"""

# fhir json structure
# allergy_intolerance_json = {
#     "resourceType": "AllergyIntolerance",
#     "identifier": [
#       {
#         "type": {
#           "coding": [
#             {
#               "system": "identifier_type_code_system",
#               "code": "identifier_type_code",
#               "display": "identifier_type_code_display"
#             }
#           ],
#           "text": "identifier_type_code_text"
#         },
#         "assigner": {
#           "reference": "Organization/47d6a1e2-778d-4dd1-babc-04ffd2d4cda4"
#         }
#       }
#     ],
#     "clinicalStatus": {
#       "coding": [
#         {
#           "system": "clinical_status_code_system",
#           "code": "clinical_status_code",
#           "display": "clinical_status_code_display"
#         }
#       ],
#       "text": "clinical_status_text"
#     },
#     "verificationStatus": {
#       "coding": [
#         {
#           "system": "verification_status_code_system",
#           "code": "verification_status_code",
#           "display": "verification_status_code_display"
#         }
#       ],
#       "text": "verification_status_text"
#     },
#     "type": "allergy",
#     "category": [
#       "food"
#     ],
#     "code": {
#       "coding": [
#         {
#           "system": "procedure_code_system",
#           "code": "procedure_code",
#           "display": "procedure_code_display"
#         }
#       ],
#       "text": "procedure_code_text"
#     },
#    "criticality": "low",
#     "patient": {
#       "reference": "Patient/ba13f91c-9e6a-4945-99ad-d813a3902953"
#     },
#     "encounter": {
#       "reference": "Encounter/28fae577-1c2e-4216-9038-2be4beda2da2"
#     },
#     "onsetPeriod": {
#       "start": "2014-05-15T05:57:03.903-05:00",
#       "end": "2014-05-15T05:57:03.903-05:00"
#     },
#     "recordedDate": "2014-05-15T05:57:03.903-05:00",
#    "recorder": {
#       "reference": "Practitioner/35048967-da22-442c-910c-65c01a13c5c4"
#     },
# "lastOccurrence": "12-12-2020",
#     "reaction": [
#       {
#         "manifestation": [
#           {
#             "coding": [
#               {
#                 "system": "manifestation_code_system",
#                 "code": "manifestation_code",
#                 "display": "manifestation_code_display"
#               }
#             ],
#             "text": "manifestation_text"
#           }
#         ],
#         "description": "reaction_desc",
#         "onset" : "12-12-2020",
#         "severity": "mild"
#       }
#     ]
#   }
# # build schema
# df=spark.read.json(spark.sparkContext.parallelize([allergy_intolerance_json]))
# schema_json = df.schema.json()
# ALLERGY_INTOLERANCE_SCHEMA = StructType.fromJson(json.loads(allergy_intolerance_json))

ALLERGY_INTOLERANCE_SCHEMA = StructType(
    [
        StructField("category", ArrayType(StringType(), True), True),
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
        StructField("criticality", StringType(), True),
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
        StructField("onsetDateTime", StringType(), True),
        StructField("lastOccurrence", StringType(), True),
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
            "reaction",
            ArrayType(
                StructType(
                    [
                        StructField("description", StringType(), True),
                        StructField(
                            "manifestation",
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
                        StructField("onset", StringType(), True),
                        StructField("severity", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("recordedDate", StringType(), True),
        StructField(
            "recorder",
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
        StructField("type", StringType(), True),
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
