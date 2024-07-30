FROM python:3.9.14-slim

WORKDIR /usr/src/app

RUN pip install --no-cache-dir boto3 click

COPY aws_s3_sync.py entrypoint.sh ./

ENTRYPOINT ["./entrypoint.sh"]