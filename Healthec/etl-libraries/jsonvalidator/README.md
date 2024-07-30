
## jsonvalidator

A python library to validate json fields with provided rules in the config file.

### Install

```sh
# from source

pip install -e .

```

### Usage

#### From Local Storage

```python
import os
from jsonvalidator import validate


script_dir = os.path.dirname(os.path.realpath(__file__))
data_fp = os.path.join(script_dir, "data/provider_canonical.json")
config_fp = os.path.join(script_dir, "config/provider_validation_config.yaml")
if validate(config_fp, data_fp, print_issues=True):
    print("Valid data file!")
else:
    print("Data file has issues!")
```

#### From AWS S3

Export AWS credentials,

```
export AWS_KEY_ID=<access_key> AWS_SECRET_KEY=<access_secret>
```


```python
import os
from jsonvalidator import validate


data_fp = "s3://hec-sandbox-datapipeline-bucket/jsonvalidator/data/provider_canonical.json"
config_fp = "s3://hec-sandbox-datapipeline-bucket/jsonvalidator/config/provider_validation_config.yaml"
if validate(config_fp, data_fp, print_issues=True):
    print("Valid data file!")
else:
    print("Data file has issues!")
```
