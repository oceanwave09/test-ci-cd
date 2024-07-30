import json
import pytest
from pathlib import Path
from fhirtransformer.transformer import FHIRTransformer


class TestAllergyIntolerance:

    test_cases = [
        (
            "tests/data/allergy_intolerance_tc1.json",
            "tests/expected_outputs/allergy_intolerance_tc1.json",
        ),
    ]

    @pytest.mark.parametrize("test_data_fp, expected_output_fp", test_cases)
    def test_allergy_intolerance(self, test_data_fp, expected_output_fp):
        transfomer = FHIRTransformer()
        transfomer.load_template("allergy_intolerance.j2")
        
        data = json.load(open(test_data_fp))
        rendered_resource = transfomer.render_resource("AllergyIntolerance", data)

        actual_output = json.loads(rendered_resource)
        expected_output = json.load(open(expected_output_fp))
        assert actual_output == expected_output
