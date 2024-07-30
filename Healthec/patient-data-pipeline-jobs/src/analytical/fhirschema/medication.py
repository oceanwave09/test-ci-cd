from pyspark.sql.types import ArrayType, StringType, StructField, StructType, FloatType

"""
Schema for fhir resource : schema will be based on the fhir template.
"""
# fhir json structure
# medication_json = {
#     "resourceType": "Medication",
#     "id": "medication_fhir_id",
#     "identifier": [
#         {
#             "type": {
#                 "coding": [
#                     {
#                         "system": "identifier_type_code_system",
#                         "code": "identifier_type_code",
#                         "display": "identifier_type_code_display",
#                     },
#                     {"system": "identifier_type_code_system", "code": "identifier_source_file_name"},
#                 ]
#             },
#             "system": "identifier_system",
#             "value": "identifier_value",
#             "assigner": {
#                 "reference": "Organization/identifier_assigner_fhir_id",
#                 "type": "Organization",
#                 "id": "identifier_assigner_id",
#             },
#         }
#     ],
#     "code": {
#         "coding": [{"system": "procedure_code_system", "code": "procedure_code", "display": "procedure_code_display"}],
#         "text": "procedure_code_text",
#     },
#     "status": "active",
#     "ingredient": [
#         {
#             "strength": {
#                 "numerator": {"value": "num_value", "unit": "num_unit", "system": "num_system", "code": "num_code"},
#                 "denominator": {
#                     "value": "denum_value",
#                     "unit": "denum_unit",
#                     "system": "denum_system",
#                     "code": "denum_code",
#                 },
#             }
#         }
#     ],
#     "form": {
#         "coding": [{"system": "form_code_system", "code": "form_code", "display": "form_code_display"}],
#         "text": "form_code_text",
#     },
#     "batch": {"lotNumber": "lot_number", "expirationDate": "2014-05-15T05:57:03.903-05:00"},
# }


# # build schema
# df=spark.read.json(spark.sparkContext.parallelize([medication_json]))
# schema_json = df.schema.json()
# MEDICATION_SCHEMA = StructType.fromJson(json.loads(medication_json))

MEDICATION_SCHEMA = StructType(
    [
        StructField(
            "batch",
            StructType(
                [StructField("expirationDate", StringType(), True), StructField("lotNumber", StringType(), True)]
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
            "form",
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
                                    )
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
            "ingredient",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "strength",
                            StructType(
                                [
                                    StructField(
                                        "denominator",
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
                                        "numerator",
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
                        )
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("resourceType", StringType(), True),
        StructField("status", StringType(), True),
    ]
)
