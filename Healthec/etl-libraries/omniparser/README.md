
## OmniParser

A python wrapper for [Omniparser](https://github.com/jf-tech/omniparser) which can also streams files directly from cloud storage like AWS S3, Azure Blob Storage to the parser.

### Install

```sh
# from source

pip install -e .

```

### Usage

#### From Local Storage

```python
from omniparser.parser import OmniParser


parser = OmniParser("CCLF_CCLF8")
parser.transform("data/cclf/cclf8.txt", "outputs/cclf/cclf8.json")
```

#### From AWS S3

Export AWS credentials,

```
export AWS_KEY_ID=dummykey AWS_SECRET_KEY=dummysecret
```

Omniparser streams the input file line by line directly from `test-bucket` S3 bucket and output also written directly into the bucket.

```python
from omniparser.parser import OmniParser


parser = OmniParser("CCLF_CCLF8")
parser.transform("s3://test-bucket/cclf8.txt", "s3://test-bucket/cclf8.json")
```