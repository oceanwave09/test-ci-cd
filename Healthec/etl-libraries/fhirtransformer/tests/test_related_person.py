import json
import pytest
from pathlib import Path
from fhirtransformer.transformer import FHIRTransformer


class TestPatient:

    test_cases = [
        (
            "tests/data/related_person_tc1.json",
            "tests/expected_outputs/related_person_tc1.json",
        ),
    ]

    @pytest.mark.parametrize("test_data_fp, expected_output_fp", test_cases)
    def test_patient(self, test_data_fp, expected_output_fp):
        test_data_path = Path(test_data_fp)
        transfomer = FHIRTransformer()
        transfomer.load_template("related_person.j2")
        
        data = json.load(open(test_data_fp))
        rendered_resource = transfomer.render_resource("RelatedPerson", data)

        actual_output = json.loads(rendered_resource)
        expected_output = json.load(open(expected_output_fp))
        assert actual_output == expected_output
