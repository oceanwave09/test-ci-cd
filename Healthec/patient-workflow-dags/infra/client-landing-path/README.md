### initiate client landing paths

This script creates the landing directories in datapipeline S3 bucket for clients with supported file format.


#### Run

1. Export AWS credential in environment variables

2. Execute the script

```
python initiate_client_landing_paths.py -c config/development/client_config.yaml -f config/development/file_format_config.yaml

```
