from fhirtransformer.transformer import FHIRTransformer

transfomer = FHIRTransformer()
transfomer.load_template("patient.j2")

input = {
    "health_record_key": "HEC2091",
    "ssn": "983748232",
    "firstname": "None",
    "lastname": "Jackson",
    "gender": "male",
    "dob": "1967-07-01",
    "active": "true",
    "street_address_1": "132, Erewhon St",
    "city": "Kingston",
    "state": "New York",
    "zip": "12401",
    "phone_mobile": "(03) 3410 5613",
    # "email": "michael.jackson@gmail.com",
    "race": "White",
    "race_code": "2106-3",
    "race_display": "White",
    "ethnicity": "Hispanic or Latino",
    "ethnicity_code": "2135-2",
    # "ethnicity_display": "Hispanic or Latino",
    # "sub_group_id": "NECQ1032",
}

print(transfomer.render_resource("Patient", input))
