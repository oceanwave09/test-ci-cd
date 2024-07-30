from pyspark.sql.types import ArrayType, StringType, StructField, StructType, BooleanType

"""
Schema for fhir resource : schema will be based on the fhir template.
"""
# fhir json structure
# patient_json = {
#     "resourceType": "Patient",
#     "id": "patient_fhir_id",
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
#     "active": "true",
#     "name": [
#         {
#             "use": "official",
#             "family": "last_name",
#             "given": ["first_name", "middle_initial"],
#             "prefix": ["prefix"],
#             "suffix": ["suffix"],
#         }
#     ],
#     "telecom": [{"system": "telecom_system", "value": "telecom_value", "use": "telecom_use"}],
#     "gender": "gender",
#     "birthDate": "2014-05-15T05:57:03.903-05:00",
#     "deceasedBoolean": "true",
#     "deceasedDateTime": "2014-05-15T05:57:03.903-05:00",
#     "address": [
#         {
#             "line": ["address_line_1", "address_line_2"],
#             "city": "address_city",
#             "district": "address_district",
#             "state": "address_state",
#             "postalCode": "address_postal_code",
#             "country": "address_country",
#         }
#     ],
#     "maritalStatus": {
#         "coding": [
#             {
#                 "system": "marital_status_code_system",
#                 "code": "marital_status_code",
#                 "display": "marital_status_code_display",
#             }
#         ],
#         "text": "marital_status_text",
#     },
#     "communication": [
#         {
#             "language": {
#                 "coding": [
#                     {"system": "language_code_system", "code": "language_code", "display": "language_code_display"}
#                 ],
#                 "text": "language_text",
#             },
#             "preferred": "true",
#         }
#     ],
#     "managingOrganization": {
#         "reference": "Organization/organization_fhir_id",
#         "id": "organization_fhir_id",
#         "type": "Organization",
#     },
#     "generalPractitioner": [
#         {"reference": "Practitioner/practitioner_fhir_id", "id": "practitioner_fhir_id", "type": "Organization"}
#     ],
#     "extension": [
#         {
#             "url": "extension_url",
#             "extension": [
#                 {
#                     "url": "extension_url",
#                     "valueCoding": {
#                         "system": "extension_value_code_system",
#                         "code": "extension_value_code",
#                         "display": "extension_value_code_display",
#                     },
#                     "valueString": "extension_value_string",
#                 }
#             ],
#         }
#     ],
#     "contact": [
#         {
#             "relationship": [
#                 {
#                     "coding": [
#                         {
#                             "system": "contact_relationship_system",
#                             "code": "contact_relationship_code",
#                             "display": "contact_relationship_display",
#                         }
#                     ],
#                     "text": "contact_relationship_text",
#                 }
#             ],
#             "name": {
#                 "use": "official",
#                 "family": "contact_lastname",
#                 "given": ["contact_firstname", "contact_middleinitials"],
#                 "prefix": ["contact_prefix"],
#                 "suffix": ["contact_suffix"],
#             },
#             "telecom": [{"system": "phone", "use": "home", "value": "contact_phone_home"}],
#             "address": {
#                 "line": ["contact_street_address_1", "contact_street_address_2"],
#                 "city": "contact_city",
#                 "district": "contact_district_code",
#                 "state": "contact_state",
#                 "postalCode": "contact_zip",
#                 "country": "contact_country",
#             },
#             "gender": "contact_gender",
#             "period": {"start": "contact_period_start_date", "end": "contact_period_end_date"},
#         }
#     ]
# }

# build schema
# df=spark.read.json(spark.sparkContext.parallelize([patient_json]))
# schema_json = df.schema.json()
# PATIENT_SCHEMA = StructType.fromJson(json.loads(patient_json))

PATIENT_SCHEMA = StructType(
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
        StructField(
            "communication",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "language",
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
                        StructField("preferred", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField(
            "contact",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "address",
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
                        StructField("gender", StringType(), True),
                        StructField(
                            "name",
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
                        StructField(
                            "period",
                            StructType(
                                [StructField("end", StringType(), True), StructField("start", StringType(), True)]
                            ),
                            True,
                        ),
                        StructField(
                            "relationship",
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
                ),
                True,
            ),
            True,
        ),
        StructField("deceasedBoolean", BooleanType(), True),
        StructField("deceasedDateTime", StringType(), True),
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
                                        StructField("valueString", StringType(), True),
                                    ]
                                ),
                                True,
                            ),
                            True,
                        ),
                        StructField("url", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("gender", StringType(), True),
        StructField(
            "generalPractitioner",
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
            "managingOrganization",
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
            "maritalStatus",
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
