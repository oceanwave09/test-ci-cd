#!/bin/bash +x

# 
if [[ "$1" != "" ]]; then
    BUILD_TAG="$1"
else
    BUILD_TAG="2.5.3-hec-1.3.0"
fi

source ./infra/docker/.env

docker build --no-cache -t registry.gitlab.com/health-ec/platform/domain/member/etl/patient-workflow-dags/airflow:$BUILD_TAG \
                --build-arg PYTHON_GITLAB_USER=$PYTHON_GITLAB_USER --build-arg PYTHON_GITLAB_TOKEN=$PYTHON_GITLAB_TOKEN --build-arg PYTHON_GITLAB_PACKAGE_ID=$PYTHON_GITLAB_PACKAGE_ID -f infra/docker/Dockerfile .

docker push registry.gitlab.com/health-ec/platform/domain/member/etl/patient-workflow-dags/airflow:$BUILD_TAG