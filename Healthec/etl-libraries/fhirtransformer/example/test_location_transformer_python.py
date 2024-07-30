from fhirtransformer.transformer import FHIRTransformer

transfomer = FHIRTransformer()
transfomer.load_template("location.j2")

input = {
    "status": "active",
    "name": "Patient Home",
    "alias": "Home",
    "description": "Patient Home",
    "mode": "kind",
    "type_code": "PTRES",
    "type_display": "Patient Residence",
    "physical_type_code": "ho",
    "physical_type_display": "House",
    "organization_id": "1c8ed3d8-6bca-47e7-aad4-2c65c7961099",
}

print(transfomer.render_resource("Location", input))
