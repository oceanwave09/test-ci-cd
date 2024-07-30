from pyspark.sql.types import ArrayType, BooleanType, StringType, StructField, StructType

"""
Schema for fhir resource : schema will be based on the fhir template.
"""
# fhir json structure
# practitioner_role_json = {
#   "resourceType": "PractitionerRole",
#   "id": "practitioner_role_fhir_id",
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
#   "active": true,
#   "period": {
#     "start": "2014-05-15T05:57:03.903-05:00",
#     "end": "2014-05-15T05:57:03.903-05:00"
#   },
#   "practitioner": {
#     "reference": "Practitioner/provider_fhir_id",
#     "id": "provider_fhir_id",
#     "type": "Practitioner"
#   },
#   "organization": {
#     "reference": "Organization/practice_fhir_id",
#     "id": "practice_fhir_id",
#     "type": "Organization"
#   },
#   "code": [
#     {
#       "coding": [
#         {
#           "system": "role_code_system",
#           "code": "role_code",
#           "display": "role_code_display"
#         }
#       ],
#       "text": "role_text"
#     }
#   ],
#   "specialty": [
#     {
#       "code": {
#         "coding": [
#           {
#             "system": "specialty_code_system",
#             "code": "specialty_code",
#             "display": "specialty_code_display"
#           }
#         ],
#         "text": "specialty_text"
#       }
#     }
#   ],
#   "location": [
#     {
#       "reference": "Location/facility_fhir_id",
#       "id": "facility_fhir_id",
#       "type": "Location"
#     }
#   ]
# }

# build schema
# PRACTITIONER_ROLE_SCHEMA = StructType.fromJson(json.loads(practitioner_role_json))

PRACTITIONER_ROLE_SCHEMA = StructType(
    [
        StructField("active", BooleanType(), True),
        StructField(
            "code",
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
            "location",
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
            "organization",
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
            StructType([StructField("end", StringType(), True), StructField("start", StringType(), True)]),
            True,
        ),
        StructField(
            "practitioner",
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
        StructField(
            "specialty",
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
