from pyspark.sql.types import ArrayType, StringType, StructField, StructType, DecimalType, BooleanType, FloatType

"""
Schema for fhir resource : schema will be based on the fhir template.
"""
# fhir json structure
# coverage_json = {
#   "resourceType": "Coverage",
#   "id": "coverage_fhir_id",
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
#   "status": "active",
#   "kind": "insurance",
#   "type": {
#     "coding": [
#       {
#         "system": "type_code_system",
#         "code": "type_code",
#         "display": "type_code_display"
#       }
#     ],
#     "text": "type_text"
#   },
#   "subscriber":{
#     "reference": "Patient/patient_fhir_id"
# },
#   "beneficiary": {
#     "reference": "Patient/patient_fhir_id",
#     "id": "patient_fhir_id",
#     "type": "Patient"
#   },
#   "dependent": "dependent",
#   "relationship": {
#     "coding": [
#       {
#         "system": "relationship_code_system",
#         "code": "relationship_code",
#         "display": "relationship_code_display"
#       }
#     ],
#     "text": "relationship_text"
#   },
#   "period": {
#     "start": "2014-05-15T05:57:03.903-05:00",
#     "end": "2014-05-15T05:57:03.903-05:00"
#   },
#   "insurer": {
#     "reference": "Organization/payer_fhir_id",
#     "id": "payer_fhir_id",
#     "type": "Organization"
#   },
#   "class": [
#     {
#       "type": {
#         "coding": [
#           {
#             "system": "class_type_code_system",
#             "code": "class_type_code",
#             "display": "class_type_code_display"
#           }
#         ],
#         "text": "class_type_text"
#       },
#       "value": "value",
#       "name": "name"
#     }
#   ],
#   "network": "network",
#   "insurancePlan": {
#     "reference": "InsurancePlan/insurance_plan_fhir_id",
#     "id": "insurance_plan_fhir_id",
#     "type": "InsurancePlan"
#   },
#   "policyHolder": {
#       "reference": "RelatedPerson/policy_holder_id "
#    },
#     "costToBeneficiary": [
#         {
#             "valueMoney": {
#             "value": "copay_money_value",
#             "currency": "copay_money_currency"
#             },
#             "valueQuantity": {
#             "value": "copay_quantity_value",
#             "unit": "copay_quantity_unit",
#             "system": "copay_quantity_system",
#             "code": "copay_quantity_code"
#             },
#             "type": {
#             "coding": [
#                 {
#                 "system": "copay_type_system",
#                 "code": "copay_type_code",
#                 "display": "copay_type_display"
#                 }
#             ],
#             "text": "data.copay_type_text"
#             }
#         }
#     ]
# }


# build schema
# df=spark.read.json(spark.sparkContext.parallelize([coverage_json]))
# schema_json = df.schema.json()
# COVERAGE_SCHEMA = StructType.fromJson(json.loads(coverage_json))

COVERAGE_SCHEMA = StructType(
    [
        StructField(
            "beneficiary",
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
            "class",
            ArrayType(
                StructType(
                    [
                        StructField("name", StringType(), True),
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
                        StructField(
                            "value",
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
                                    StructField("use", StringType(), True),
                                    StructField("value", StringType(), True),
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
            "costToBeneficiary",
            ArrayType(
                StructType(
                    [
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
                        StructField(
                            "valueMoney",
                            StructType(
                                [
                                    StructField("currency", StringType(), True),
                                    StructField("value", FloatType(), True),
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
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("dependent", StringType(), True),
        StructField(
            "extension",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "extension",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("url", StringType(), True),
                                        StructField("valueBoolean", BooleanType(), True),
                                        StructField("valueCode", StringType(), True),
                                        StructField(
                                            "valueCoding",
                                            StructType(
                                                [
                                                    StructField("code", StringType(), True),
                                                    StructField("display", StringType(), True),
                                                    StructField("system", StringType(), True),
                                                ]
                                            ),
                                            True,
                                        ),
                                        StructField("valueInteger", DecimalType(), True),
                                        StructField(
                                            "valuePeriod",
                                            StructType(
                                                [
                                                    StructField("end", StringType(), True),
                                                    StructField("start", StringType(), True),
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
                        StructField("url", StringType(), True),
                        StructField("valueBoolean", BooleanType(), True),
                        StructField("valueCode", StringType(), True),
                        StructField(
                            "valueCoding",
                            StructType(
                                [
                                    StructField("code", StringType(), True),
                                    StructField("display", StringType(), True),
                                    StructField("system", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                        StructField("valueInteger", DecimalType(), True),
                        StructField(
                            "valuePeriod",
                            StructType(
                                [
                                    StructField("end", StringType(), True),
                                    StructField("start", StringType(), True),
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
            "insurancePlan",
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
        StructField("kind", StringType(), True),
        StructField("network", StringType(), True),
        StructField("order", StringType(), True),
        StructField(
            "period",
            StructType(
                [
                    StructField("end", StringType(), True),
                    StructField("start", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "policyHolder",
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
            "relationship",
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
        StructField("resourceType", StringType(), True),
        StructField("status", StringType(), True),
        StructField(
            "subscriber",
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
