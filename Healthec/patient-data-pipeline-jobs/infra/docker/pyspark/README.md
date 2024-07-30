
## Build PySpark Base Image

```sh
SPARK_VERSION="3.4.1"
HADOOP_MAJOR_VERSION="3"

# download spark binary
wget -q --show-progress https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}.tgz
tar -zxf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}.tgz
rm -f spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}.tgz

cd spark-${SPARK_VERSION}-bin-hadoop${HADOOP_MAJOR_VERSION}

# important:
# update pyspark dockerfile(kubernetes/dockerfiles/spark/bindings/python/Dockerfile)
# 1. Add wget, git in apt install

# build spark-py base image
./bin/docker-image-tool.sh -r registry.gitlab.com/health-ec/platform/domain/member/etl/patient-data-pipeline-jobs -t 3.4.1-base -p ./kubernetes/dockerfiles/spark/bindings/python/Dockerfile build
docker login registry.gitlab.com -u $PYTHON_GITLAB_USER -p $PYTHON_GITLAB_TOKEN
docker push registry.gitlab.com/health-ec/platform/domain/member/etl/patient-data-pipeline-jobs/spark-py:3.4.1-base
```