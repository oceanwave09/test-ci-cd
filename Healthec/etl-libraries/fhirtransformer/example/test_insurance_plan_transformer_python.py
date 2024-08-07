from fhirtransformer.transformer import FHIRTransformer

transfomer = FHIRTransformer()
transfomer.load_template("insurance_plan.j2")

input = {
    "source_id": "8576713",
    "name": "Humana PPO",
    "plan_type": "medical",
    "status": "active",
    "owned_organization_id": "d4dc14ea-a5a2-481e-aa0d-3432e0b562eb",
    "administered_organization_id": "2bb088a4-5259-47b2-8d0b-8d67d4af8fca",
    "coverage_area": [
        {
            "location_id": "dda84174-22dc-4af5-a5f0-2e9f828365c8"
        }
    ],
    "coverages": [
        {
            "type_text": "Accident",
            "benefits": [
                {
                    "type_text": "Emergency",
                    "max_value": "1000"
                }
            ]
        }
    ]
}

print(transfomer.render_resource("InsurancePlan", input))
