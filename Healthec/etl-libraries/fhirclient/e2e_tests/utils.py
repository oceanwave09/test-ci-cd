# coding: utf-8

import random
import string


def random_integer_by_size(x):
    return "{0:0{x}d}".format(random.randint(0, 10**x - 1), x=x)


def generate_random_string(size: int = 16):
    return "".join(random.choices(string.ascii_letters + string.digits, k=size))


def get_org_type(type: str = None):
    org_type = {}
    if type == "Provider":
        org_type = [{
            "coding":[
                {
                    "system": "http: //terminology.hl7.org/CodeSystem/organization-type",
                    "code": "prov",
                    "display": "Healthcare Provider"
                }
            ]
        }]
    else:
        org_type = [{
            "coding":[
                {
                    "system": "urn:oid: 2.16.840.1.113883.2.4.15.1060",
                    "code": "V6",
                    "display": "Care"
                }
            ]
        }]
    return org_type