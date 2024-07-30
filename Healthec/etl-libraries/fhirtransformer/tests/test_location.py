import json
import pytest
from pathlib import Path
from fhirtransformer.transformer import FHIRTransformer


class TestLocation:

    test_cases = [
        (
            "tests/data/location_tc1.json",
            "tests/expected_outputs/location_tc1.json",
        ),
    ]

    @pytest.mark.parametrize("test_data_fp, expected_output_fp", test_cases)
    def test_location(self, test_data_fp, expected_output_fp):
        test_data_path = Path(test_data_fp)
        transfomer = FHIRTransformer()
        transfomer.load_template("location.j2")
        
        data = json.load(open(test_data_fp))
        rendered_resource = transfomer.render_resource("Location", data)

        actual_output = json.loads(rendered_resource)
        expected_output = json.load(open(expected_output_fp))
        assert actual_output == expected_output
