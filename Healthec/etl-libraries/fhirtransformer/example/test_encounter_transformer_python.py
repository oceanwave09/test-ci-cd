from fhirtransformer.transformer import FHIRTransformer

transfomer = FHIRTransformer()
transfomer.load_template("encounter.j2")

input = {
    "source_id": "8576713",
    "status": "finished",
    "class_code": "IMP",
    "class_display": "inpatient encounter",
    "location_id": "d4dc14ea-a5a2-481e-aa0d-3432e0b562eb",
    "practitioner_id": "2bb088a4-5259-47b2-8d0b-8d67d4af8fca",
    "patient_id": "dda84174-22dc-4af5-a5f0-2e9f828365c8",
    "period_start_date": "2015-01-17T16:00:00+10:00",
    "period_end_date": "2015-01-17T16:30:00+10:00",
    "discharge_disposition_display": "Home",
    "extensions": [
        {
            "url": "http://healthec.com/extensions/patients/flags/hospice",
            "value_boolean": "true"
        },
        {
            "url": "http://healthec.com/extensions/patients/flags/long-term-institutional-care",
            "value_boolean": "true"
        }
    ],
}

print(transfomer.render_resource("Encounter", input))