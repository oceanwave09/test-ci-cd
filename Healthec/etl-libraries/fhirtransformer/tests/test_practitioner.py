import json
import pytest
from pathlib import Path
from fhirtransformer.transformer import FHIRTransformer


class TestPractitionerTransformer:

    test_cases = [
        (
            "tests/data/practitioner_tc1.json",
            "tests/expected_outputs/practitioner_tc1.json",
        ),
    ]

    @pytest.mark.parametrize("test_data_fp, expected_output_fp", test_cases)
    def test_practitioner_transformer(self, test_data_fp, expected_output_fp):
        test_data_path = Path(test_data_fp)
        transfomer = FHIRTransformer()
        transfomer.load_template("practitioner.j2")
        
        data = json.load(open(test_data_fp))
        rendered_resource = transfomer.render_resource("Practitioner", data)

        actual_output = json.loads(rendered_resource)
        expected_output = json.load(open(expected_output_fp))
        assert actual_output == expected_output
