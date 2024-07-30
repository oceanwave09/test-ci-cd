import os
from jsonvalidator.validation import Validator


script_dir = os.path.dirname(os.path.realpath(__file__))
data_fp = os.path.join(script_dir, "data/provider_canonical.json")
config_fp = os.path.join(script_dir, "config/provider_validation_config.yaml")

obj = Validator(yaml_file=config_fp)
error_list = obj.get_error_list(
    data=obj.read_file(file_path=data_fp, flatten=True, format="json")
)
print(error_list)
