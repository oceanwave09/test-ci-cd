import os
import re
import json
import gzip
import pytest
from pathlib import Path
from omniparser.parser import OmniParser

ACTUAL_OUTPUTS_DIR = "tests/actual_outputs"


class TestOmniParserConfigs:
    test_cases = [
        (
            "schemas/healthec/practice_roster/practice_config.json",
            "tests/data/practice_roster/practice_roster.txt",
            "tests/expected_outputs/practice_roster/practice_roster.json",
            "disable",
        ),
        (
            "schemas/healthec/provider_roster/provider_config.json",
            "tests/data/provider_roster/provider_roster.txt",
            "tests/expected_outputs/provider_roster/provider_roster.json",
            "disable",
        ),
        (
            "schemas/healthec/837/claim_config.json",
            "tests/data/837/837_original.txt",
            "tests/expected_outputs/837/837_original.json",
            "disable",
        ),
        (
            "schemas/healthec/cclf/cclf8_config.json",
            "tests/data/cclf/cclf8.txt",
            "tests/expected_outputs/cclf/cclf8.json",
            "disable",
        ),
        (
            "schemas/baco/provider_config.json",
            "tests/data/baco/baco_provider.txt",
            "tests/expected_outputs/baco/provider.json",
            "disable",
        ),
        (
            "schemas/baco/hap/beneficiary_config.json",
            "tests/data/baco/hap/hap_beneficiary.txt",
            "tests/expected_outputs/baco/hap/beneficiary.json",
            "disable",
        ),
        (
            "schemas/baco/humana/beneficiary_config.json",
            "tests/data/baco/humana/humana_beneficiary.txt",
            "tests/expected_outputs/baco/humana/beneficiary.json",
            "disable",
        ),
        (
            "schemas/baco/hap/medical_claim_config.json",
            "tests/data/baco/hap/hap_medical_claim.txt",
            "tests/expected_outputs/baco/hap/medical_claim.json",
            "disable",
        ),
        (
            "schemas/baco/humana/medical_claim_config.json",
            "tests/data/baco/humana/humana_medical_claim.txt",
            "tests/expected_outputs/baco/humana/medical_claim.json",
            "disable",
        ),
        (
            "schemas/nucleo/hewmana/medical_claim_config.json",
            "tests/data/nucleo/hewmana/hap_medical_claim.txt",
            "tests/expected_outputs/nucleo/hewmana/medical_claim.json",
            "disable",
        ),
        (
            "schemas/nucleo/horighzon/medical_claim_config.json",
            "tests/data/nucleo/horighzon/humana_medical_claim.txt",
            "tests/expected_outputs/nucleo/horighzon/medical_claim.json",
            "disable",
        ),
        (
            "schemas/nucleo/hewmana/rx_claim_config.json",
            "tests/data/nucleo/hewmana/hap_rx_claim.txt",
            "tests/expected_outputs/nucleo/hewmana/rx_claim.json",
            "disable",
        ),
        (
            "schemas/nucleo/horighzon/rx_claim_config.json",
            "tests/data/nucleo/horighzon/humana_rx_claim.txt",
            "tests/expected_outputs/nucleo/horighzon/rx_claim.json",
            "disable",
        ),
        (
            "schemas/baco/hap/rx_claim_config.json",
            "tests/data/baco/hap/hap_rx_claim.txt",
            "tests/expected_outputs/baco/hap/rx_claim.json",
            "disable",
        ),
        (
            "schemas/baco/humana/rx_claim_config.json",
            "tests/data/baco/humana/humana_rx_claim.txt",
            "tests/expected_outputs/baco/humana/rx_claim.json",
            "disable",
        ),
        (
            "schemas/healthec/ecw/allergy_config.json",
            "tests/data/ecw/allergy.txt",
            "tests/expected_outputs/ecw/allergy.json",
            "disable",
        ),
        (
            "schemas/healthec/ecw/diagnosis_config.json",
            "tests/data/ecw/diagnosis.txt",
            "tests/expected_outputs/ecw/diagnosis.json",
            "disable",
        ),
        (
            "schemas/healthec/ecw/encounter_config.json",
            "tests/data/ecw/encounter.txt",
            "tests/expected_outputs/ecw/encounter.json",
            "disable",
        ),
        (
            "schemas/healthec/ecw/examination_config.json",
            "tests/data/ecw/examination.txt",
            "tests/expected_outputs/ecw/examination.json",
            "disable",
        ),
        (
            "schemas/healthec/ecw/facility_config.json",
            "tests/data/ecw/facility.txt",
            "tests/expected_outputs/ecw/facility.json",
            "disable",
        ),
        (
            "schemas/healthec/ecw/family_history_config.json",
            "tests/data/ecw/familyhistory.txt",
            "tests/expected_outputs/ecw/familyhistory.json",
            "disable",
        ),
        (
            "schemas/healthec/ecw/hospitalization_config.json",
            "tests/data/ecw/hospitalization.txt",
            "tests/expected_outputs/ecw/hospitalization.json",
            "disable",
        ),
        (
            "schemas/healthec/ecw/hpi_config.json",
            "tests/data/ecw/hpi.txt",
            "tests/expected_outputs/ecw/hpi.json",
            "disable",
        ),
        (
            "schemas/healthec/ecw/immunization_config.json",
            "tests/data/ecw/immunization.txt",
            "tests/expected_outputs/ecw/immunization.json",
            "disable",
        ),
        (
            "schemas/healthec/ecw/medical_history_config.json",
            "tests/data/ecw/medicalhistory.txt",
            "tests/expected_outputs/ecw/medicalhistory.json",
            "disable",
        ),
        (
            "schemas/healthec/ecw/medication_config.json",
            "tests/data/ecw/medication.txt",
            "tests/expected_outputs/ecw/medication.json",
            "disable",
        ),
        (
            "schemas/healthec/ecw/obgyn_history_config.json",
            "tests/data/ecw/obgynhistory.txt",
            "tests/expected_outputs/ecw/obgynhistory.json",
            "disable",
        ),
        (
            "schemas/healthec/ecw/order_and_result_config.json",
            "tests/data/ecw/orderandresult.txt",
            "tests/expected_outputs/ecw/orderandresult.json",
            "disable",
        ),
        (
            "schemas/healthec/ecw/patient_demographic_config.json",
            "tests/data/ecw/patientdemographic.txt",
            "tests/expected_outputs/ecw/patientdemographic.json",
            "disable",
        ),
        (
            "schemas/healthec/ecw/physical_exam_config.json",
            "tests/data/ecw/physicalexam.txt",
            "tests/expected_outputs/ecw/physicalexam.json",
            "disable",
        ),
        (
            "schemas/healthec/ecw/practice_user_config.json",
            "tests/data/ecw/practiceuser.txt",
            "tests/expected_outputs/ecw/practiceuser.json",
            "disable",
        ),
        (
            "schemas/healthec/ecw/preventive_config.json",
            "tests/data/ecw/preventive.txt",
            "tests/expected_outputs/ecw/preventive.json",
            "disable",
        ),
        (
            "schemas/healthec/ecw/problem_list_config.json",
            "tests/data/ecw/problemlist.txt",
            "tests/expected_outputs/ecw/problemlist.json",
            "disable",
        ),
        (
            "schemas/healthec/ecw/procedure_config.json",
            "tests/data/ecw/procedure.txt",
            "tests/expected_outputs/ecw/procedure.json",
            "disable",
        ),
        (
            "schemas/healthec/ecw/procedure_code_config.json",
            "tests/data/ecw/procedurecode.txt",
            "tests/expected_outputs/ecw/procedurecode.json",
            "disable",
        ),
        (
            "schemas/healthec/ecw/referral_config.json",
            "tests/data/ecw/referral.txt",
            "tests/expected_outputs/ecw/referral.json",
            "disable",
        ),
        (
            "schemas/healthec/ecw/social_history_config.json",
            "tests/data/ecw/socialhistory.txt",
            "tests/expected_outputs/ecw/socialhistory.json",
            "disable",
        ),
        (
            "schemas/healthec/ecw/surgical_history_config.json",
            "tests/data/ecw/surgicalhistory.txt",
            "tests/expected_outputs/ecw/surgicalhistory.json",
            "disable",
        ),
        (
            "schemas/healthec/ecw/vital_config.json",
            "tests/data/ecw/vital.txt",
            "tests/expected_outputs/ecw/vital.json",
            "disable",
        ),
        (
            "schemas/healthec/qrda/clinical_config.json",
            "tests/data/qrda/qrda.json",
            "tests/expected_outputs/qrda/qrda.json",
            "disable",
        ),
        (
            "schemas/healthec/gap/allergy_config.json",
            "tests/data/gap/allergy.csv",
            "tests/expected_outputs/gap/allergy.json",
            "disable",
        ),
        (
            "schemas/healthec/gap/care_config.json",
            "tests/data/gap/care.csv",
            "tests/expected_outputs/gap/care.json",
            "disable",
        ),
        (
            "schemas/healthec/gap/immunization_config.json",
            "tests/data/gap/immunization.csv",
            "tests/expected_outputs/gap/immunization.json",
            "disable",
        ),
        (
            "schemas/healthec/gap/vital_config.json",
            "tests/data/gap/vital.csv",
            "tests/expected_outputs/gap/vital.json",
            "disable",
        ),
        (
            "schemas/healthec/gap/demographic_config.json",
            "tests/data/gap/demographic.csv",
            "tests/expected_outputs/gap/demographic.json",
            "disable",
        ),
        (
            "schemas/healthec/gap/lab_config.json",
            "tests/data/gap/lab.csv",
            "tests/expected_outputs/gap/lab.json",
            "disable",
        ),
        (
            "schemas/healthec/gap/medicalcondition_config.json",
            "tests/data/gap/medicalcondition.csv",
            "tests/expected_outputs/gap/medicalcondition.json",
            "disable",
        ),
        (
            "schemas/healthec/gap/medication_config.json",
            "tests/data/gap/medication.csv",
            "tests/expected_outputs/gap/medication.json",
            "disable",
        ),
        (
            "schemas/healthec/gap/smoking_config.json",
            "tests/data/gap/smoking.csv",
            "tests/expected_outputs/gap/smoking.json",
            "disable",
        ),
        (
            "schemas/healthec/gap/sdoh_config.json",
            "tests/data/gap/sdoh.csv",
            "tests/expected_outputs/gap/sdoh.json",
            "disable",
        ),
        (
            "schemas/healthec/ncqa_hedis/diag_config.json",
            "tests/data/ncqa_hedis/diag.txt",
            "tests/expected_outputs/ncqa_hedis/diag.json",
            "disable",
        ),
        (
            "schemas/healthec/ncqa_hedis/lab_config.json",
            "tests/data/ncqa_hedis/lab.txt",
            "tests/expected_outputs/ncqa_hedis/lab.json",
            "disable",
        ),
        (
            "schemas/healthec/ncqa_hedis/lishist_config.json",
            "tests/data/ncqa_hedis/lishist.txt",
            "tests/expected_outputs/ncqa_hedis/lishist.json",
            "disable",
        ),
        (
            "schemas/healthec/ncqa_hedis/member-en_config.json",
            "tests/data/ncqa_hedis/member-en.txt",
            "tests/expected_outputs/ncqa_hedis/member-en.json",
            "disable",
        ),
        (
            "schemas/healthec/ncqa_hedis/member-gm_config.json",
            "tests/data/ncqa_hedis/member-gm.txt",
            "tests/expected_outputs/ncqa_hedis/member-gm.json",
            "disable",
        ),
        (
            "schemas/healthec/ncqa_hedis/mmdf_config.json",
            "tests/data/ncqa_hedis/mmdf1.txt",
            "tests/expected_outputs/ncqa_hedis/mmdf.json",
            "disable",
        ),
        (
            "schemas/healthec/ncqa_hedis/obs_config.json",
            "tests/data/ncqa_hedis/obs.txt",
            "tests/expected_outputs/ncqa_hedis/obs.json",
            "disable",
        ),
        (
            "schemas/healthec/ncqa_hedis/pharm-c_config.json",
            "tests/data/ncqa_hedis/pharm-c.txt",
            "tests/expected_outputs/ncqa_hedis/pharm-c.json",
            "disable",
        ),
        (
            "schemas/healthec/ncqa_hedis/pharm_config.json",
            "tests/data/ncqa_hedis/pharm.txt",
            "tests/expected_outputs/ncqa_hedis/pharm.json",
            "disable",
        ),
        (
            "schemas/healthec/ncqa_hedis/proc_config.json",
            "tests/data/ncqa_hedis/proc.txt",
            "tests/expected_outputs/ncqa_hedis/proc.json",
            "disable",
        ),
        (
            "schemas/healthec/ncqa_hedis/provider_config.json",
            "tests/data/ncqa_hedis/provider.txt",
            "tests/expected_outputs/ncqa_hedis/provider.json",
            "disable",
        ),
        (
            "schemas/healthec/ncqa_hedis/visit-e_config.json",
            "tests/data/ncqa_hedis/visit-e.txt",
            "tests/expected_outputs/ncqa_hedis/visit-e.json",
            "disable",
        ),
        (
            "schemas/healthec/ncqa_hedis/visit_config.json",
            "tests/data/ncqa_hedis/visit.txt",
            "tests/expected_outputs/ncqa_hedis/visit.json",
            "disable",
        ),
        (
            "schemas/healthec/hl7/ADT_A01_config.json",
            "tests/data/hl7/ADT_A01.hl7",
            "tests/expected_outputs/hl7/ADT_A01.json",
            "disable",
        ),
        (
            "schemas/healthec/hl7/ORU_R01_config.json",
            "tests/data/hl7/ORU_R01.hl7",
            "tests/expected_outputs/hl7/ORU_R01.json",
            "disable",
        ),
        (
            "schemas/healthec/hl7/VXU_V04_config.json",
            "tests/data/hl7/VXU_V04.hl7",
            "tests/expected_outputs/hl7/VXU_V04.json",
            "disable",
        ),
        (
            "schemas/healthec/radaid/patient_config.json",
            "tests/data/radaid/patient.csv",
            "tests/expected_outputs/radaid/patient.json",
            "disable",
        ),
        (
            "schemas/healthec/radaid/ptoutreach_config.json",
            "tests/data/radaid/ptoutreach.csv",
            "tests/expected_outputs/radaid/ptoutreach.json",
            "disable",
        ),
        (
            "schemas/healthec/radaid/brcascr_config.json",
            "tests/data/radaid/brcascr.csv",
            "tests/expected_outputs/radaid/brcascr.json",
            "disable",
        ),
        (
            "schemas/healthec/radaid/brcadiag_config.json",
            "tests/data/radaid/brcadiag.csv",
            "tests/expected_outputs/radaid/brcadiag.json",
            "disable",
        ),
        (
            "schemas/healthec/radaid/brcaproc_config.json",
            "tests/data/radaid/brcaproc.csv",
            "tests/expected_outputs/radaid/brcaproc.json",
            "disable",
        ),
        (
            "schemas/healthec/radaid/brcatx_config.json",
            "tests/data/radaid/brcatx.csv",
            "tests/expected_outputs/radaid/brcatx.json",
            "disable",
        ),
        (
            "schemas/healthec/radaid/cxcascr_config.json",
            "tests/data/radaid/cxcascr.csv",
            "tests/expected_outputs/radaid/cxcascr.json",
            "disable",
        ),
        (
            "schemas/healthec/radaid/cxcadiag_config.json",
            "tests/data/radaid/cxcadiag.csv",
            "tests/expected_outputs/radaid/cxcadiag.json",
            "disable",
        ),
        (
            "schemas/healthec/radaid/cxcaproc_config.json",
            "tests/data/radaid/cxcaproc.csv",
            "tests/expected_outputs/radaid/cxcaproc.json",
            "disable",
        ),
        (
            "schemas/healthec/radaid/cxcatx_config.json",
            "tests/data/radaid/cxcatx.csv",
            "tests/expected_outputs/radaid/cxcatx.json",
            "disable",
        ),
        (
            "schemas/healthec/cclf_beneficiary/cclf_bene_alr1_1_mdvc_config.json",
            "tests/data/cclf_beneficiary/cclf_bene_alr1_1.txt",
            "tests/expected_outputs/cclf_beneficiary/cclf_bene_alr1_1.json",
            "disable",
        ),
        (
            "schemas/healthec/cclf_beneficiary/cclf_bene_alr1_2_config.json",
            "tests/data/cclf_beneficiary/cclf_bene_alr1_2.txt",
            "tests/expected_outputs/cclf_beneficiary/cclf_bene_alr1_2.json",
            "disable",
        ),
        (
            "schemas/healthec/cclf_beneficiary/cclf_bene_alr1_3_config.json",
            "tests/data/cclf_beneficiary/cclf_bene_alr1_3.txt",
            "tests/expected_outputs/cclf_beneficiary/cclf_bene_alr1_3.json",
            "disable",
        ),
        (
            "schemas/healthec/cclf_beneficiary/cclf_bene_alr1_4_config.json",
            "tests/data/cclf_beneficiary/cclf_bene_alr1_4.txt",
            "tests/expected_outputs/cclf_beneficiary/cclf_bene_alr1_4.json",
            "disable",
        ),
        (
            "schemas/healthec/cclf_beneficiary/cclf_bene_alr1_5_config.json",
            "tests/data/cclf_beneficiary/cclf_bene_alr1_5.txt",
            "tests/expected_outputs/cclf_beneficiary/cclf_bene_alr1_5.json",
            "disable",
        ),
        (
            "schemas/healthec/cclf_beneficiary/cclf_bene_alr1_6_config.json",
            "tests/data/cclf_beneficiary/cclf_bene_alr1_6.txt",
            "tests/expected_outputs/cclf_beneficiary/cclf_bene_alr1_6.json",
            "disable",
        ),
        (
            "schemas/healthec/cclf_beneficiary/cclf_bene_alr1_7_config.json",
            "tests/data/cclf_beneficiary/cclf_bene_alr1_7.txt",
            "tests/expected_outputs/cclf_beneficiary/cclf_bene_alr1_7.json",
            "disable",
        ),
        (
            "schemas/healthec/cclf_beneficiary/cclf_bene_alr1_9_config.json",
            "tests/data/cclf_beneficiary/cclf_bene_alr1_9.txt",
            "tests/expected_outputs/cclf_beneficiary/cclf_bene_alr1_9.json",
            "disable",
        ),
        (
            "schemas/healthec/clinical/patient_config.json",
            "tests/data/clinical/patientinformation.txt",
            "tests/expected_outputs/clinical/patient.json",
            "disable",
        ),
        (
            "schemas/healthec/clinical/lab_result_config.json",
            "tests/data/clinical/patientlabresult.txt",
            "tests/expected_outputs/clinical/lab_result.json",
            "disable",
        ),
        (
            "schemas/healthec/clinical/medication_config.json",
            "tests/data/clinical/medicationinformation.txt",
            "tests/expected_outputs/clinical/medication.json",
            "disable",
        ),
        (
            "schemas/healthec/clinical/visit_codeset_config.json",
            "tests/data/clinical/patientvisitcodeset.txt",
            "tests/expected_outputs/clinical/visit_codeset.json",
            "disable",
        ),
        (
            "schemas/healthec/clinical/visit_config.json",
            "tests/data/clinical/patientvisitinformation.txt",
            "tests/expected_outputs/clinical/visit.json",
            "disable",
        ),
        (
            "schemas/healthec/clinical/payer_config.json",
            "tests/data/clinical/payerinformation.txt",
            "tests/expected_outputs/clinical/payer.json",
            "disable",
        ),
        (
            "schemas/healthec/clinical/vital_config.json",
            "tests/data/clinical/vitals.txt",
            "tests/expected_outputs/clinical/vital.json",
            "disable",
        ),
    ]

    @classmethod
    def setup_method(cls):
        if not os.path.exists(ACTUAL_OUTPUTS_DIR):
            os.mkdir(ACTUAL_OUTPUTS_DIR)

    @pytest.mark.parametrize(
        "schema, test_data_fp, expected_output_fp, compression", test_cases
    )
    def test_omniparser_configs(
        self, schema, test_data_fp, expected_output_fp, compression
    ):
        test_data_path = Path(test_data_fp)
        if compression != "disable":
            actual_output_fp = os.path.join(
                ACTUAL_OUTPUTS_DIR, test_data_path.name + ".json.gz"
            )
        else:
            actual_output_fp = os.path.join(
                ACTUAL_OUTPUTS_DIR, test_data_path.name + ".json"
            )
        try:
            parser = OmniParser(schema, {"data_key": "TestEncrypt12345"})
            if compression != "disable":
                parser.transform(test_data_fp, actual_output_fp, compression)
            else:
                parser.transform(test_data_fp, actual_output_fp)

            expected_output = json.load(open(expected_output_fp))
            if compression != "disable":
                with open(actual_output_fp, "rb") as f:
                    actual_output = json.loads(f.read().decode("ascii"))
            else:
                actual_output = json.load(open(actual_output_fp))

            # assert length
            assert len(actual_output) == len(expected_output)
            for i in range(len(actual_output)):
                actual_entry = actual_output[i]
                expected_entry = expected_output[i]

                # actual assert row id
                actual_row_id = actual_entry.pop("row_id")
                assert re.search(
                    r"[0-9a-f]{8}\-[0-9a-f]{4}\-4[0-9a-f]{3}\-[89ab][0-9a-f]{3}\-[0-9a-f]{12}",
                    actual_row_id,
                )

                # pop row id
                expected_entry.pop("row_id")

                # assert entry
                assert actual_entry == expected_entry

        finally:
            # pass
            os.remove(actual_output_fp)
