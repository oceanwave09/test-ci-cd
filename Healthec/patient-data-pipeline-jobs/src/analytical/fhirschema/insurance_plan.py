from pyspark.sql.types import ArrayType, StringType, StructField, StructType, FloatType

"""
Schema for fhir resource : schema will be based on the fhir template.
"""
# fhir json structure
# insurance_plan_json = {
#   "resourceType": "InsurancePlan",
#   "id": "insurance_plan_fhir_id",
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
#   "alias": "enter_data",
#   "type": [
#     {
#       "coding": [
#         {
#           "system": "type_code_system",
#           "code": "type_code",
#           "display": "type_code_display"
#         }
#       ],
#       "text": "type_text"
#     }
#   ],
#   "name": "insurance_plan_name",
#   "period": {
#     "start": "2014-05-15T05:57:03.903-05:00",
#     "end": "2014-05-15T05:57:03.903-05:00"
#   },
#   "ownedBy": {
#     "reference": "Organization/payer_fhir_id",
#     "id": "payer_fhir_id",
#     "type": "Organization"
#   },
#   "contact": [
#     {
#       "name": {
#         "use": "official",
#         "family": "contact_last_name",
#         "given": [
#           "contact_first_name",
#           "contact_middle_initial"
#         ],
#         "suffix": [
#           "contact_suffix"
#         ],
#         "prefix": [
#           "contact_prefix"
#         ]
#       },
#       "telecom": [
#         {
#           "system": "telecom_system",
#           "use": "telecom_use",
#           "value": "telecom_value"
#         }
#       ]
#     }
#   ],
#   "Coverage": [
#     {
#       "type": {
#         "coding": [
#           {
#             "system": "coverage_type_code_system",
#             "code": "coverage_type_code",
#             "display": "coverage_type_code_display"
#           }
#         ],
#         "text": "coverage_type_text"
#       },
#       "network": [
#         {
#           "reference": "Organization/network_fhir_id",
#           "id": "network_fhir_id",
#           "type": "Organization"
#         }
#       ],
#       "benefit": [
#         {
#           "type": {
#             "coding": [
#               {
#                 "system": "coverage_benefit_type_code_system",
#                 "code": "coverage_benefit_type_code",
#                 "display": "coverage_benefit_type_code_display"
#               }
#             ],
#             "text": "coverage_benefit_type_text"
#           },
#           "requirement": "coverage_benefit_requirement",
#           "limit": [
#             {
#               "value": {
#                 "value": 23232,
#                 "unit": "coverage_limit_value_unit"
#               }
#             }
#           ]
#         }
#       ]
#     }
#   ]
# }


# build schema
# df=spark.read.json(spark.sparkContext.parallelize([insurance_plan_json]))
# schema_json = df.schema.json()
# INSURANCE_PLAN_SCHEMA = StructType.fromJson(json.loads(insurance_plan_json))

"""
Schema for fhir resource : schema will be based on the fhir template.
"""
INSURANCE_PLAN_SCHEMA = StructType(
    [
        StructField(
            "coverage",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "benefit",
                            ArrayType(
                                StructType(
                                    [
                                        StructField(
                                            "limit",
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            "value",
                                                            StructType(
                                                                [
                                                                    StructField(
                                                                        "unit",
                                                                        StringType(),
                                                                        True,
                                                                    ),
                                                                    StructField(
                                                                        "value",
                                                                        FloatType(),
                                                                        True,
                                                                    ),
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
                                        StructField("requirement", StringType(), True),
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
                        StructField(
                            "network",
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
            True,
        ),
        StructField(
            "administeredBy",
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
        StructField("name", StringType(), True),
        StructField("alias", StringType(), True),
        StructField(
            "ownedBy",
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
            "period",
            StructType(
                [
                    StructField("end", StringType(), True),
                    StructField("start", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("status", StringType(), True),
        StructField(
            "coverageArea",
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
