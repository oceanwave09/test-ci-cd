from fhirtransformer.transformer import FHIRTransformer

transfomer = FHIRTransformer()
transfomer.load_template("practitioner.j2")

input = {
    "ssn": "983748232",
    "npi": "737257957",
    "firstname": "Michael",
    "lastname": "Jackson",
    "gender": "male",
    "dob": "1967-07-01",
    "active": "true",
    "street_address_1": "132, Erewhon St",
    "city": "Kingston",
    "state": "New York",
    "zip": "12401",
    "phone_mobile": "(03) 3410 5613",
    "email": "michael.jackson@gmail.com",
}

print(transfomer.render_resource("Practitioner", input))
