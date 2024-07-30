from fhirtransformer.transformer import FHIRTransformer

transfomer = FHIRTransformer()
transfomer.load_template("coverage.j2")

input = {
    "mrn": "A123456780",
    "mrn_system": "http://ehic.com/insurer/123456789/member",
    "status": "active",
    "kind": "insurance",
    "subscriber_fhir_id": "f41ba4cf-a1d4-4735-b23a-b017d7d0dc75",
    "beneficiary_fhir_id": "f2b8db7e-c0a4-40d2-a245-93ea68699394",
    "payor_fhir_id": "7533a02e-37ca-4ac1-bddd-d1e2c0c7df01",
    "relationship_code": "self",
    "relationship_display": "Self",
    "period_start_date": "2023-01-01",
    "period_end_date": "2023-12-31"
}

print(transfomer.render_resource("Coverage", input))
