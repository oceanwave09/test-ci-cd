from pyspark.sql.types import ArrayType, StringType, StructField, StructType

"""
Schema for fhir resource : schema will be based on the fhir template.
"""
# # fhir json structure
# procedure_json = {
#     "resourceType": "Procedure",
#     "id": "procedure_fhir_id",
#     "identifier": [
#         {
#             "type": {
#                 "coding": [
#                     {
#                         "system": "identifier_type_code_system",
#                         "code": "identifier_type_code",
#                         "display": "identifier_type_code_display",
#                     }
#                 ],
#                 "text": "identifier_type_code_text",
#             },
#             "system": "identifier_system",
#             "value": "identifier_value",
#             "assigner": {
#                 "reference": "Organization/identifier_assigner_fhir_id",
#                 "id": "identifier_assigner_fhir_id",
#                 "type": "Organization",
#             },
#         }
#     ],
#     "status": "completed",
#     "category": {
#         "coding": [{"system": "category_code_system", "code": "category_code", "display": "category_code_display"}],
#         "text": "category_text",
#     },
#     "code": {
#         "coding": [
#             {"system": "procedure_code_system", "code": "procedure_code", "display": "procedure_code_display"}
#         ],
#         "text": "procedure_code_text",
#     },
#     "subject": {"reference": "Patient/patient_fhir_id", "id": "patient_fhir_id", "type": "Patient"},
#     "encounter": {"reference": "Encounter/encounter_fhir_id", "id": "encounter_fhir_id", "type": "Encounter"},
#     "performedDateTime": "2014-05-15T05:57:03.903-05:00",
#     "performedPeriod": {"start": "2014-05-15T05:57:03.903-05:00", "end": "2014-05-15T05:57:03.903-05:00"},
#     "performer": [
#         {
#             "function": {
#                 "coding": [{"system": "function_system", "code": "function_code", "display": "function_display"}],
#                 "text": "function_text",
#             },
#             "actor": {
#                 "reference": "Practitioner/practitioner_fhir_id",
#                 "id": "practitioner_fhir_id",
#                 "type": "Practitioner",
#             },
#         }
#     ],
#     "location": {"reference": "Location/location_fhir_id", "id": "location_fhir_id", "type": "Location"},
#     "modifierExtension": [
#         {
#             "url": "url",
#             "valueCodeableConcept": {
#                 "coding": [{"system": "code_system", "code": "code", "display": "code_display"}],
#                 "text": "code_text",
#             },
#         }
#     ],
#     "reasonReference": [{"reference": "Condition/condition_fhir_id", "id": "condition_fhir_id", "type": "Condition"}],
#     "reasonCode": [
#         {
#             "coding": [{"system": "reason_code_system", "code": "reason_code", "display": "reason_code_display"}],
#             "text": "reason_code_text",
#         }
#     ],
#     "bodySite": [
#         {
#             "coding": [
#                 {"system": "bodysite_code_system", "code": "bodysite_code", "display": "bodysite_code_display"}
#             ],
#             "text": "bodysite_text",
#         }
#     ],
#     "outcome": {
#         "coding": [{"system": "outcome_code_system", "code": "outcome_code", "display": "outcome_code_display"}],
#         "text": "outcome_code_text",
#     },
# }

# build schema
# df=spark.read.json(spark.sparkContext.parallelize([procedure_json]))
# schema_json = df.schema.json()
# PROCEDURE_SCHEMA = StructType.fromJson(json.loads(procedure_json))

PROCEDURE_SCHEMA = StructType(
    [
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
                        StructField("value", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
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
            "modifierExtension",
            ArrayType(
                StructType(
                    [
                        StructField("url", StringType(), True),
                        StructField(
                            "valueCodeableConcept",
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
            "outcome",
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
        StructField("performedDateTime", StringType(), True),
        StructField(
            "performedPeriod",
            StructType([StructField("end", StringType(), True), StructField("start", StringType(), True)]),
            True,
        ),
        StructField(
            "performer",
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
            "reasonReference",
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
        StructField("resourceType", StringType(), True),
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
    ]
)
