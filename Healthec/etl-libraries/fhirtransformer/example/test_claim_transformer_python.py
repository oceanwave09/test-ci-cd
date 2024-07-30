from fhirtransformer.transformer import FHIRTransformer

transfomer = FHIRTransformer()
transfomer.load_template("claim.j2")

input = {
    "source_id": "107274487100",
    "status": "active",
    "type_code": "oral",
    "patient_id": "10008769",
    "use": "claim",
    "billable_period_start": "06-02-2023",
    "billable_period_end": "07-02-2023",
    "created_date_time": "07-02-2023",
    "insurer_organization_id": "8675594",
    "provider_practitioner_id": "12401",
    "priority_code": "normal",
    "funds_reserve_code": "RED",
    "prescription_medication_request_id": "PMR02",
    "orig_prescription_medication_request_id": "orginal",
    "payee_type_code": "HLOO231",
    "payee_practitioner_id": "2135-2",
    # "payee_organization_id": "ORG897",
    "service_request_id": "SERV-22",
    "facility_location_id": "USA472",
    "carteam_sequence": "1",
    "careteam_provider_practitioner_id": "cppid123",
    "careteam_role_code":"blue",
    "careteam_qualification_code": "MBA",
    "diagnoses": [
        {
            "sequence":"32",
            "diagnosis_code": "diag_test_22",
            "diagnosis_condition_id": "diag123",
            "drg_code": "3456-6"
        },
        {
            "sequence":"33",
            "diagnosis_code": "diag_test_23",
            "type_code": "C",
            "on_admission_code": "18oac104"
        }     
    ],
    "procedures": [
        {
            "sequence":"23",
            "procedure_code": "proc_test_12",
            "procedure_id": "proc123"
        },
        {
            "sequence":"24",
            "procedure_code": "proc_test_13",
            "date_time": "20-02-2023"
        }
        
    ],
    "insurance_sequence": "5",
    "insurance_focal": "88.8",
    "insurance_coverage_id": "IOBA345",
    "accident_date_time": "08-02-2023",
    "accident_type_code": "test_acc",
    "accident_line_1": "street 1",
    "accident_city":"New York",
    "accident_location_id": "NY-23",
    "service_lines": [
        {
            "sequence":"42",
            "careteam_sequence": "4245",
            "revenue_code": "REV_56",
            "category_code": "CAT_67",
            "service_date_time": "20-02-2023",
            "service_period_start": "20-02-2023",
            "encounter_id": "ENC_01"
        },
        {
            "sequence":"43",
            "diagnosis_sequence": "4356",
            "revenue_code": "REV_57",
            "location_id": "FACI_234",
            "location_code": "LOC_TEST",
            "encounter_id": "ENC_02"
        },
        {
            "sequence":"44",
            "procedure_sequence": "4245",
            "category_code": "CAT_68",
            "quantity_value": "210",
            "unit_value": "23",
            "net_value":"34",
            "body_site_code": "34",
            "encounter_id": "ENC_03"
        }     
    ]
}

print(transfomer.render_resource("Claim", input))
