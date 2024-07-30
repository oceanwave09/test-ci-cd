#!/bin/bash +x

source ./infra/docker/.env

if [[ "$1" != "" ]]; then
    BUILD_TAG="$1"
else
    BUILD_TAG="1.0.0"
fi

# build python dependencies zip file
sh ./infra/docker/build_dependencies.sh

# build patient data piepline docker image
docker build --no-cache -t registry.gitlab.com/health-ec/platform/domain/auth/etl/user-data-pipeline-jobs/user-data-pipeline:$BUILD_TAG \
                --build-arg PYTHON_GITLAB_USER=$PYTHON_GITLAB_USER --build-arg PYTHON_GITLAB_TOKEN=$PYTHON_GITLAB_TOKEN --build-arg PYTHON_GITLAB_PACKAGE_ID=$PYTHON_GITLAB_PACKAGE_ID -f infra/docker/Dockerfile .

docker push registry.gitlab.com/health-ec/platform/domain/auth/etl/user-data-pipeline-jobs/user-data-pipeline:$BUILD_TAG

# clean up resources collected to create docker image
rm -f ./infra/docker/resources/python-deps.zip
