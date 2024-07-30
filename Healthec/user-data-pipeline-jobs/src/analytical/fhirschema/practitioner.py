from pyspark.sql.types import ArrayType, BooleanType, StringType, StructField, StructType

"""
Schema for fhir resource : schema will be based on the fhir template.
"""
# fhir json structure
# practitioner_json = {
#   "resourceType": "Practitioner",
#   "id": "practitioner_fhir_id",
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
#   "name": [
#     {
#       "use": "official",
#       "family": "last_name",
#       "given": [
#         "first_name",
#         "middle_initial"
#       ],
#       "prefix": [
#         "prefix"
#       ],
#       "suffix": [
#         "suffix"
#       ]
#     }
#   ],
#   "telecom": [
#     {
#       "system": "telecom_system",
#       "value": "telecom_value",
#       "use": "telecom_use"
#     }
#   ],
#   "address": [
#     {
#       "line": [
#         "address_line_1",
#         "address_line_2"
#       ],
#       "city": "address_city",
#       "district": "address_district",
#       "state": "address_state",
#       "postalCode": "address_postal_code",
#       "country": "address_country"
#     }
#   ],
#   "gender": "gender",
#   "birthDate": "2014-05-15",
#   "qualification": [
#     {
#       "code": {
#         "coding": [
#           {
#             "system": "qualification_code_system",
#             "code": "qualification_code",
#             "display": "qualification_code_display"
#           }
#         ],
#         "text": "qualification_text"
#       }
#     }
#   ]
# }

# build schema
# PRACTITIONER_SCHEMA = StructType.fromJson(json.loads(practitioner_json))

PRACTITIONER_SCHEMA = StructType(
    [
        StructField("active", BooleanType(), True),
        StructField(
            "address",
            ArrayType(
                StructType(
                    [
                        StructField("city", StringType(), True),
                        StructField("country", StringType(), True),
                        StructField("district", StringType(), True),
                        StructField("line", ArrayType(StringType(), True), True),
                        StructField("postalCode", StringType(), True),
                        StructField("state", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("birthDate", StringType(), True),
        StructField("gender", StringType(), True),
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
            "name",
            ArrayType(
                StructType(
                    [
                        StructField("family", StringType(), True),
                        StructField("given", ArrayType(StringType(), True), True),
                        StructField("prefix", ArrayType(StringType(), True), True),
                        StructField("suffix", ArrayType(StringType(), True), True),
                        StructField("use", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            "qualification",
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
                        )
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("resourceType", StringType(), True),
        StructField(
            "telecom",
            ArrayType(
                StructType(
                    [
                        StructField("system", StringType(), True),
                        StructField("use", StringType(), True),
                        StructField("value", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
    ]
)
