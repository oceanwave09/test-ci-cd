from fhirtransformer.transformer import FHIRTransformer

transfomer = FHIRTransformer()
transfomer.load_template("organization_affiliation.j2")

input = {
    "organization_id": "bb7a0594-6be2-44e9-a1d4-dab0c74f91aa",
    "participating_organization_id": "e5289839-d2bf-4f11-bd53-49e6bbf4838b",
    "network_organization_id": "fbcec387-6684-4867-8002-d30b54cabc96",
    "active": "true",
    "period_start_date": "2023-01-01",
    "period_end_date": "2023-12-31",
    "role_code": "payer",
    "role_display": "Payer",
    "specialty_code": "394802001",
    "specialty_display": "General medicine"
}

print(transfomer.render_resource("OrganizationAffiliation", input))