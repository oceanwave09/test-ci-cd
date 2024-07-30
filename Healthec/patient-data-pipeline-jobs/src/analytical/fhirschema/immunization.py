from pyspark.sql.types import ArrayType, StringType, StructField, StructType, BooleanType, FloatType, DecimalType

"""
Schema for fhir resource : schema will be based on the fhir template.
"""
# fhir json structure
# immunization_json = {
#   "resourceType": "Immunization",
#   "id": "immunization_fhir_id",
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
#   "status": "completed",
#   "vaccineCode": {
#     "coding": [
#       {
#         "system": "vaccine_code_system",
#         "code": "vaccine_code",
#         "display": "vaccine_code_display"
#       }
#     ],
#     "text": "vaccine_code_text"
#   },
#   "patient": {
#     "reference": "Patient/patient_fhir_id",
#     "id": "patient_fhir_id",
#     "type": "Patient"
#   },
#   "encounter": {
#     "reference": "Encounter/encounter_fhir_id",
#     "id": "encounter_fhir_id",
#     "type": "Encounter"
#   },
#   "reasonCode" : [
#     {
#       "coding" : [
#         {
#           "system": "reason_system",
#           "code": "reason_code",
#           "display": "reason_display"
#         }
#       ],
#       "text": "reason_text"
#     }
#   ],
#   "statusReason" : [
#     {
#       "coding" : [
#         {
#           "system": "status_reason_system",
#           "code": "status_reason_code",
#           "display": "status_reason_display"
#         }
#       ],
#       "text": "status_reason_text"
#     }
#   ],
#   "programEligibility" : [
#     {
#       "coding" : [
#         {
#           "system": "program_eligibility_system",
#           "code": "program_eligibility_code",
#           "display": "program_eligibility_display"
#         }
#       ],
#       "text": "program_eligibility_text"
#     }
#   ],
#   "occurrenceDateTime": "2014-05-15T05:57:03.903-05:00",
#   "recorded": "2014-05-15T05:57:03.903-05:00",
#   "location": {
#     "reference": "Location/facility_fhir_id",
#     "id": "facility_fhir_id",
#     "type": "Location"
#   },
#   "manufacturer": {
#     "reference": "Organization/manufacturer_fhir_id",
#     "id": "manufacturer_fhir_id",
#     "type": "Organization"
#   },
#   "lotNumber": "lot_number",
#   "expirationDate": "2014-05-15",
#   "site": {
#     "coding": [
#       {
#         "system": "site_code_system",
#         "code": "site_code",
#         "display": "site_code_display"
#       }
#     ],
#     "text": "site_code_text"
#   },
#   "route": {
#     "coding": [
#       {
#         "system": "route_code_system",
#         "code": "route_code",
#         "display": "route_code_display"
#       }
#     ],
#     "text": "route_code_text"
#   },
#   "doseQuantity": {
#     "value": 10,
#     "unit": "value_unit",
#     "system": "value_unit_system",
#     "code": "value_unit_code"
#   },
#   "performer": [
#     {
#       "actor": {
#         "reference": "Practitioner/practitioner_fhir_id",
#         "id": "practitioner_fhir_id",
#         "type": "Practitioner"
#       }
#     }
#   ],
#   "reasonReference": [
#     {
#       "reference": "Condition/condition_fhir_id",
#       "id": "condition_fhir_id",
#       "type": "Condition"
#     }
#   ],
#   "extension": [
#         {
#             "url": "url",
#             "extension": [
#                 {
#                     "url": "url",
#                     "valueBoolean": true,
#                     "valueCode": "value_code",
#                     "valueCoding": {
#                         "system": "value_coding_system",
#                         "code": "value_coding",
#                         "display": "value_coding_display"
#                     },
#                     "valueInteger": 42,
#                     "valuePeriod": {"start": "value_period_start", "end": "value_period_end"},
#                     "valueString": "value"
#                 }
#             ],
#             "valueBoolean": true,
#             "valueCode": "value_set_code",
#             "valueCoding": {
#                 "system": "value_set_coding_system",
#                 "code": "value_set_coding",
#                 "display": "value_set_coding_display"
#             },
#             "valueInteger": 42,
#             "valuePeriod": {"start": "value_set_period_start", "end": "value_set_period_end"},
#             "valueString": "value_set"
#         }
#     ]
# }

# # build schema
# # build schema
# df=spark.read.json(spark.sparkContext.parallelize([immunization_json]))
# schema_json = df.schema.json()
# IMMUNIZATION_SCHEMA = StructType.fromJson(json.loads(immunization_json))

IMMUNIZATION_SCHEMA = StructType(
    [
        StructField(
            "doseQuantity",
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
        StructField("expirationDate", StringType(), True),
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
        StructField("lotNumber", StringType(), True),
        StructField(
            "manufacturer",
            StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("reference", StringType(), True),
                    StructField("type", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("occurrenceDateTime", StringType(), True),
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
            "programEligibility",
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
        StructField("recorded", StringType(), True),
        StructField("resourceType", StringType(), True),
        StructField(
            "route",
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
            "site",
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
        StructField("status", StringType(), True),
        StructField(
            "statusReason",
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
            "vaccineCode",
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
