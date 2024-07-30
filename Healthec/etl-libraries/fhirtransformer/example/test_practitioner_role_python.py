from fhirtransformer.transformer import FHIRTransformer

transfomer = FHIRTransformer()
transfomer.load_template("practitioner_role.j2")

input = {
    "organization_id": "cee79fb5-f6b5-49e0-ad1b-5aecc9198c3d",
    "practitioner_id": "02a2a694-cf3a-4141-9bd7-8f5c54793f35",
    "active": "true",
    "specialty_code": "394802001",
    "specialty_display": "General medicine",
    "phone_mobile": "(03) 3410 5613",
    "email": "michael.jackson@gmail.com",
}

print(transfomer.render_resource("PractitionerRole", input))
