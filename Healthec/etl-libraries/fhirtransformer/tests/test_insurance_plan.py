import json
import pytest
from pathlib import Path
from fhirtransformer.transformer import FHIRTransformer


class TestInsurancePlan:

    test_cases = [
        (
            "tests/data/insurance_plan_tc1.json",
            "tests/expected_outputs/insurance_plan_tc1.json",
        ),
    ]

    @pytest.mark.parametrize("test_data_fp, expected_output_fp", test_cases)
    def test_insurance_plan(self, test_data_fp, expected_output_fp):
        test_data_path = Path(test_data_fp)
        transfomer = FHIRTransformer()
        transfomer.load_template("insurance_plan.j2")
        
        data = json.load(open(test_data_fp))
        rendered_resource = transfomer.render_resource("InsurancePlan", data)

        actual_output = json.loads(rendered_resource)
        expected_output = json.load(open(expected_output_fp))
        assert actual_output == expected_output
