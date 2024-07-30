import json
import pytest
from pathlib import Path
from fhirtransformer.transformer import FHIRTransformer


class TestFamilyMemberHistory:

    test_cases = [
        (
            "tests/data/family_member_history_tc1.json",
            "tests/expected_outputs/family_member_history_tc1.json",
        ),
    ]

    @pytest.mark.parametrize("test_data_fp, expected_output_fp", test_cases)
    def test_family_member_history(self, test_data_fp, expected_output_fp):
        transfomer = FHIRTransformer()
        transfomer.load_template("family_member_history.j2")
        
        data = json.load(open(test_data_fp))
        rendered_resource = transfomer.render_resource("FamilyMemberHistory", data)

        actual_output = json.loads(rendered_resource)
        expected_output = json.load(open(expected_output_fp))
        assert actual_output == expected_output
