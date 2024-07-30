FROM registry.gitlab.com/health-ec/platform/domain/member/etl/patient-data-pipeline-jobs/spark-py:3.4.1-base

ARG PYTHON_GITLAB_USER
ARG PYTHON_GITLAB_TOKEN
ARG PYTHON_GITLAB_PACKAGE_ID

USER root

COPY ./infra/docker/requirements.txt /

RUN pip install -r /requirements.txt

# RUN pip install fhirclient fhirtransformer --index-url https://${PYTHON_GITLAB_USER}:${PYTHON_GITLAB_TOKEN}@gitlab.com/api/v4/projects/${PYTHON_GITLAB_PACKAGE_ID}/packages/pypi/simple

RUN python3 -m pip install --no-cache-dir "git+https://${PYTHON_GITLAB_USER}:${PYTHON_GITLAB_TOKEN}@gitlab.com/health-ec/platform/core/libraries/etl-libraries.git@main#egg=fhirclient&subdirectory=fhirclient"

RUN python3 -m pip install --no-cache-dir "git+https://${PYTHON_GITLAB_USER}:${PYTHON_GITLAB_TOKEN}@gitlab.com/health-ec/platform/core/libraries/etl-libraries.git@main#egg=fhirtransformer&subdirectory=fhirtransformer"

RUN wget --quiet https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar -O /opt/spark/jars/postgresql-42.6.0.jar
RUN wget --quiet https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -O /opt/spark/jars/hadoop-aws-3.3.4.jar
RUN wget --quiet https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client/3.3.4/hadoop-client-3.3.4.jar -O /opt/spark/jars/hadoop-client-3.3.4.jar
RUN wget --quiet https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -O /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar
RUN wget --quiet https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar -O /opt/spark/jars/delta-core_2.12-2.4.0.jar
RUN wget --quiet https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar -O /opt/spark/jars/delta-storage-2.4.0.jar

ENV PATH=$PATH:/app
ENV PYTHONPATH /app

RUN great_expectations -y init

COPY ./infra/docker/great_expectations.yml gx/

COPY ./infra/docker/resources/python-deps.zip /app/

ADD ./src/jobs /app/jobs
