import json
import pytest
from pathlib import Path
from fhirtransformer.transformer import FHIRTransformer


class TestMedicationRequest:

    test_cases = [
        (
            "tests/data/medication_request_tc1.json",
            "tests/expected_outputs/medication_request_tc1.json",
        ),
    ]

    @pytest.mark.parametrize("test_data_fp, expected_output_fp", test_cases)
    def test_medication_request(self, test_data_fp, expected_output_fp):
        transfomer = FHIRTransformer()
        transfomer.load_template("medication_request.j2")
        
        data = json.load(open(test_data_fp))
        rendered_resource = transfomer.render_resource("MedicationRequest", data)

        actual_output = json.loads(rendered_resource)
        expected_output = json.load(open(expected_output_fp))
        assert actual_output == expected_output
