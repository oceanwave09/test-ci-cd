from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType


# {
#        "resourceType": "OrganizationAffiliation",
#        "id": "orgrole1",
#        "identifier": [
#          {
#            "use": "secondary",
#            "system": "http://example.org/www.foundingfathersmemorial.com",
#            "value": "service002",
#            "assigner": {
#              "reference": "http://hl7.org/fhir/ig/vhdir/Organization/foundingfathers",
#              "id": "org_fhir_id",
#              "type": "Organization",
#              "display": "Founding Fathers Memorial Hospital"
#            }
#          }
#        ],
#        "active": true,
#        "period": {
#          "start": "2018-02-09",
#          "end": "2022-02-01"
#        },
#        "organization": {
#          "reference": "http://hl7.org/fhir/ig/vhdir/Organization/foundingfathers",
#          "id": "org_fhir_id",
#          "type": "Organization",
#          "display": "Founding Fathers Memorial Hospital"
#        },
#        "participatingOrganization": {
#          "reference": "http://hl7.org/fhir/ig/vhdir/Organization/independencerehab",
#          "id": "org_fhir_id",
#          "type": "Organization",
#          "display": "Independence Rehabilitation Services, Inc."
#        },
#        "network": [
#          {
#            "reference": "http://hl7.org/fhir/ig/vhdir/Network/patriotppo",
#            "id": "org_fhir_id",
#            "type": "Organization",
#            "display": "Patriot Preferred Provider Network"
#          }
#        ],
#        "code": [
#          {
#            "coding": [
#              {
#                "system": "http://hl7.org/fhir/organization-role",
#                "code": "provider",
#                "display": "Provider"
#              }
#            ],
#            "text": "Provider of rehabilitation services"
#          }
#        ],
#        "specialty": [
#          {
#            "coding": [
#              {
#                "system": "http://snomed.info/sct",
#                "code": "394602003",
#                "display": "Rehabilitation - specialty"
#              }
#            ],
#            "text": "Rehabilitation"
#          }
#        ],
#        "location": [
#          {
#            "reference": "http://hl7.org/fhir/ig/vhdir/Location/foundingfathers1",
#            "id": "Location_fhir_id",
#            "type": "Location",
#            "display": "Founding Fathers Memorial Hospital"
#          }
#        ]
#      }

ORGANIZATION_AFFILIATION_SCHEMA = StructType(
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
                                        StructField("system", StringType(), True)
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
                                    StructField("display", StringType(), True),
                                    StructField("id", StringType(), True),
                                    StructField("reference", StringType(), True),
                                    StructField("type", StringType(), True),
                                ]
                            ),
                            True,
                        ),
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
            "location",
            ArrayType(
                StructType(
                    [
                        StructField("display", StringType(), True),
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
            "network",
            ArrayType(
                StructType(
                    [
                        StructField("display", StringType(), True),
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
                    StructField("display", StringType(), True),
                    StructField("id", StringType(), True),
                    StructField("reference", StringType(), True),
                    StructField("type", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "participatingOrganization",
            StructType(
                [
                    StructField("display", StringType(), True),
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
