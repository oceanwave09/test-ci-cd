from pyspark.sql.types import ArrayType, StringType, StructField, StructType

"""
Schema for fhir resource : schema will be based on the fhir template.
"""
# fhir json structure
# location_json = {
#     "id": "fe952d84-d98d-4cda-8c51-283a1b06b8b8",
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
#             "value": "RI11111"
#         },
#         {
#             "type": {
#                 "coding": [
#                     {
#                         "system": "identifier_type_code_system",
#                         "code": "NPI",
#                         "display": "identifier_type_code_display"
#                     }
#                 ],
#                 "text": "identifier_type_code_text"
#             },
#             "value": "NPI222222222"
#         }
#     ],
#     "status": "active",
#     "operationalStatus": {
#         "system": "http://terminology.hl7.org/CodeSystem/v2-0116",
#         "code": "operational_status_code"
#     },
#     "name": "name",
#     "alias": [
#         "alias"
#     ],
#     "description": "description",
#     "mode": "kind",
#     "type": [
#         {
#             "coding": [
#                 {
#                     "system": "type_code_system",
#                     "code": "type_code",
#                     "display": "type_code_display"
#                 }
#             ],
#             "text": "type_text"
#         }
#     ],
#     "telecom": [
#         {
#             "system": "phone",
#             "value": "telecom_value",
#             "use": "home"
#         }
#     ],
#     "address": {
#         "line": [
#             "address_line_1",
#             "address_line_2"
#         ],
#         "city": "address_city",
#         "district": "address_district",
#         "state": "address_state",
#         "postalCode": "address_postal_code",
#         "country": "address_country"
#     },
#     "physicalType": {
#         "coding": [
#             {
#                 "system": "http://terminology.hl7.org/CodeSystem/location-physical-type",
#                 "code": "ro",
#                 "display": "Room"
#             }
#         ],
#         "text": "Room"
#     },
#     "managingOrganization": {
#         "id": "47d6a1e2-778d-4dd1-babc-04ffd2d4cda4",
#         "reference": "Organization/47d6a1e2-778d-4dd1-babc-04ffd2d4cda4",
#         "type": "Organization"
#     },
#     "availabilityExceptions": "exp",
#     "resourceType": "Location"
# }
# build schema
# LOCATION_SCHEMA = StructType.fromJson(json.loads(location_json))

LOCATION_SCHEMA = StructType(
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
        StructField("alias", ArrayType(StringType(), True), True),
        StructField("availabilityExceptions", StringType(), True),
        StructField("description", StringType(), True),
        StructField("id", StringType(), True),
        StructField(
            "identifier",
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
                                                    StructField(
                                                        "code", StringType(), True
                                                    ),
                                                    StructField(
                                                        "display", StringType(), True
                                                    ),
                                                    StructField(
                                                        "system", StringType(), True
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
        StructField("mode", StringType(), True),
        StructField("name", StringType(), True),
        StructField(
            "operationalStatus",
            StructType(
                [
                    StructField("code", StringType(), True),
                    StructField("system", StringType(), True),
                    StructField("display", StringType(), True),
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
        StructField("resourceType", StringType(), True),
        StructField("status", StringType(), True),
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
