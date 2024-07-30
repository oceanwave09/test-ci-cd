FROM apache/airflow:2.5.3-python3.8

ARG PYTHON_GITLAB_USER
ARG PYTHON_GITLAB_TOKEN
ARG PYTHON_GITLAB_PACKAGE_ID

USER root

# TODO: Added to address the following issue
# ISSUE: https://github.com/apache/airflow/issues/36231 
RUN rm /etc/apt/sources.list.d/mysql.list

RUN apt-get update \
  && apt-get install -y git

USER airflow

COPY ./infra/docker/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

RUN pip install omniparser fhirclient fhirtransformer --index-url https://${PYTHON_GITLAB_USER}:${PYTHON_GITLAB_TOKEN}@gitlab.com/api/v4/projects/${PYTHON_GITLAB_PACKAGE_ID}/packages/pypi/simple

RUN python -m pip install --no-cache-dir "git+https://${PYTHON_GITLAB_USER}:${PYTHON_GITLAB_TOKEN}@gitlab.com/health-ec/platform/core/libraries/etl-libraries.git@main#egg=jsonvalidator&subdirectory=jsonvalidator"
