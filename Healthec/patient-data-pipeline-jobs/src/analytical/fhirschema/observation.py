from pyspark.sql.types import ArrayType, StringType, StructField, StructType, FloatType

"""
Schema for fhir resource : schema will be based on the fhir template.
"""
# # fhir json structure
# observation_json = {
#     "resourceType": "Observation",
#     "id": "id",
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
#                 "id": "organization_identifier_id",
#                 "type": "Organization",
#             },
#         }
#     ],
#     "status": "status",
#     "category": [
#         {"coding": [{"system": "code_system", "code": "code", "display": "code_display"}], "text": "code_text"}
#     ],
#     "code": {"coding": [{"system": "code_system", "code": "code", "display": "code_display"}], "text": "code_text"},
#     "subject": {"reference": "Patient/reference_id", "id": "reference_id", "type": "Patient"},
#     "encounter": {"reference": "Encounter/reference_id", "id": "reference_id", "type": "Encounter"},
#     "effectiveDateTime": "effective_date_time",
#     "effectivePeriod": {"start": "effective_start", "end": "effective_end"},
#     "performer": [
#         {
#             "reference": "Practitioner/practitioner_reference_id",
#             "id": "practitioner_reference_id",
#             "type": "Practitioner",
#         }
#     ],
#     "valueString": "value_string",
#     "valueQuantity": {"value": 10, "unit": "value_unit", "system": "value_system", "code": "value_code"},
#     "valueCodeableConcept": {
#         "coding": [{"system": "code_system", "code": "code", "display": "code_display"}],
#         "text": "code_text",
#     },
#     "interpretation": [
#         {"coding": [{"system": "code_system", "code": "code", "display": "code_display"}], "text": "code_text"}
#     ],
#     "bodySite": {
#         "coding": [{"system": "code_system", "code": "code", "display": "code_display"}],
#         "text": "code_text",
#     },
#     "method": {"coding": [{"system": "code_system", "code": "code", "display": "code_display"}], "text": "code_text"},
#     "referenceRange": [
#         {
#             "low": {"value": 10, "unit": "value_unit", "system": "value_system", "code": "value_code"},
#             "high": {"value": 10, "unit": "value_unit", "system": "value_system", "code": "value_code"},
#             "text": "text",
#         }
#     ],
#     "component": [
#         {
#             "code": {
#                 "coding": [{"system": "code_system", "code": "code", "display": "code_display"}],
#                 "text": "code_text",
#             },
#             "valueString": "value_string",
#             "valueQuantity": {"value": 10, "unit": "value_unit", "system": "value_system", "code": "value_code"},
#             "valueCodeableConcept": {
#                 "coding": [{"system": "code_system", "code": "code", "display": "code_display"}],
#                 "text": "code_text",
#             },
#             "interpretation": [
#                 {
#                     "coding": [{"system": "code_system", "code": "code", "display": "code_display"}],
#                     "text": "code_text",
#                 }
#             ],
#             "referenceRange": [
#                 {
#                     "low": {"value": 10, "unit": "value_unit", "system": "value_system", "code": "value_code"},
#                     "high": {"value": 10, "unit": "value_unit", "system": "value_system", "code": "value_code"},
#                     "text": "text",
#                 }
#             ],
#         }
#     ],
# }

# # build schema
# df=spark.read.json(spark.sparkContext.parallelize([observation_json]))
# schema_json = df.schema.json()
# OBSERVATION_SCHEMA = StructType.fromJson(json.loads(observation_json))

# Observation Schema
OBSERVATION_SCHEMA = StructType(
    [
        StructField(
            "bodySite",
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
            "component",
            ArrayType(
                StructType(
                    [
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
                            "interpretation",
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
                            "referenceRange",
                            ArrayType(
                                StructType(
                                    [
                                        StructField(
                                            "high",
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
                                            "low",
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
                                        StructField("text", StringType(), True),
                                    ]
                                ),
                                True,
                            ),
                            True,
                        ),
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
                        StructField(
                            "valueQuantity",
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
                        StructField("valueString", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("effectiveDateTime", StringType(), True),
        StructField(
            "effectivePeriod",
            StructType([StructField("end", StringType(), True), StructField("start", StringType(), True)]),
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
                        StructField("assigner", StructType([StructField("reference", StringType(), True)]), True),
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
            "interpretation",
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
            "method",
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
            "performer",
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
        StructField(
            "referenceRange",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "high",
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
                            "low",
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
                        StructField("text", StringType(), True),
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
        StructField(
            "valueQuantity",
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
        StructField("valueString", StringType(), True),
    ]
)
