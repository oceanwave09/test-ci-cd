from fhirtransformer.transformer import FHIRTransformer

transfomer = FHIRTransformer()
transfomer.load_template("organization.j2")

input = {
    "sub_group_id": "NECQ1032",
    "tax_id": "983748232",
    "active": "true",
    "name": "Good Health Clinic",
    "type_code": "prov",
    "type_display": "Healthcare Provider",
    "street_address_1": "132, Erewhon St",
    "city": "Kingston",
    "state": "New York",
    "zip": "12401",
    "phone_mobile": "(03) 3410 5613",
    "email": "michael.jackson@gmail.com",
}

print(transfomer.render_resource("Organization", input))
