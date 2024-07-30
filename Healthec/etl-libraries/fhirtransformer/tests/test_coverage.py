import json
import pytest
from pathlib import Path
from fhirtransformer.transformer import FHIRTransformer


class TestCoverage:

    test_cases = [
        (
            "tests/data/coverage_tc1.json",
            "tests/expected_outputs/coverage_tc1.json",
        ),
    ]

    @pytest.mark.parametrize("test_data_fp, expected_output_fp", test_cases)
    def test_coverage(self, test_data_fp, expected_output_fp):
        test_data_path = Path(test_data_fp)
        transfomer = FHIRTransformer()
        transfomer.load_template("coverage.j2")
        
        data = json.load(open(test_data_fp))
        rendered_resource = transfomer.render_resource("Coverage", data)

        actual_output = json.loads(rendered_resource)
        expected_output = json.load(open(expected_output_fp))
        assert actual_output == expected_output
