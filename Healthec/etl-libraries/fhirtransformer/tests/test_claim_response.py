import json
import pytest
from pathlib import Path
from fhirtransformer.transformer import FHIRTransformer


class TestClaimResponse:

    test_cases = [
        (
            "tests/data/claim_response_tc1.json",
            "tests/expected_outputs/claim_response_tc1.json",
        ),
    ]

    @pytest.mark.parametrize("test_data_fp, expected_output_fp", test_cases)
    def test_claim_response(self, test_data_fp, expected_output_fp):
        transfomer = FHIRTransformer()
        transfomer.load_template("claim_response.j2")
        
        data = json.load(open(test_data_fp))
        rendered_resource = transfomer.render_resource("ClaimResponse", data)

        actual_output = json.loads(rendered_resource)
        expected_output = json.load(open(expected_output_fp))
        assert actual_output == expected_output