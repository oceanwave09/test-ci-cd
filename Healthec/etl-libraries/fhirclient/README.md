## FHIRClient

Python client for HEC domain services.


### Installation

From source:

```
git clone --recursive https://gitlab.com/health-ec/architecture/prototypes/etl/libraries.git
cd fhirclient
pip install -r requirement.txt
pip install -e .
```

### Examples

#### Testing in Local

##### Prerequisites

* Run provider, user and patient service locally
```
# setup .env file

# start services

docker-compose --profile service up

# create kafka topics

docker run -it --rm --network datapipeline -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181 bitnami/kafka:latest kafka-topics.sh --create --topic provider  --bootstrap-server kafka:9092

docker run -it --rm --network datapipeline -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181 bitnami/kafka:latest kafka-topics.sh --create --topic patient  --bootstrap-server kafka:9092

docker run -it --rm --network datapipeline -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181 bitnami/kafka:latest kafka-topics.sh --create --topic user  --bootstrap-server kafka:9092

docker run -it --rm --network datapipeline -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181 bitnami/kafka:latest kafka-topics.sh --create --topic financial  --bootstrap-server kafka:9092

# stop services
docker-compose down --volumes --remove-orphans

```

##### Patient Create

```python
# client configuration
service_host  = "127.0.0.1:5001"
provider_host = "127.0.0.1:5002"
user_host = "127.0.0.1:5003"
protocol = "http"
auth_config = {"auth_disabled": True}
config = Configuration(
    protocol=protocol,
    service_host=service_host,
    provider_host=provider_host,
    user_host=user_host,
    auth_config=auth_config,
)
# create patient client
client = FHIRClient(configuration=client_config)
patient_client = Patient(client)

# create patient
patient_create = patient_client.create(patient_resource)
if patient_create and patient_create.get("id"):
    print(f"patient create successful, patient details: {patient_create}")
else:
    raise SystemExit("patient create unsuccessful")
```

#### Testing on Sandbox

##### Prerequisites

* Export auth client id and client secret
```
export FHIR_CLIENT_ID=<CLIENT-ID>
export FHIR_CLIENT_SECRET=<CLIENT-SECRET>
```

##### Patient Create

```python
# client configuration to make request with provider service
service_host = "development.healthec.com"
tenant_subdomain = "cynchealth"
protocol = "https"
auth_config = {
    "auth_host": "development.healthec.com",
    "auth_subdomain": "keycloak",
    "auth_tenant_subdomain": "cynchealth",
}
client_config = Configuration(
    protocol=protocol,
    service_host=service_host, 
    tenant_subdomain=tenant_subdomain, 
    auth_config=auth_config,
)
# create patient client
client = FHIRClient(client_config)
patient_client = Patient(client)

# create patient resource
patient_create = patient_client.create(patient_resource)
if patient_create and patient_create.get("id"):
    print(f"patient create successful, patient details: {patient_create}")
else:
    raise SystemExit("patient create unsuccessful")
```
See more [example](examples/).

* Client configuration

    - `service_host`: patient or common service host
    - `protocol`: url schema (http/https, default: http)
    - `provider_host`: provider host (optional). if not provided service_host is used (default: None)
    - `user_host`: user host (optional). if not provided service_host is used (default: None)
    - `tenant_subdomain`: tenant subdomain name (default: None)
    - `resolve_reference`: flag to enable resolving resource references from identifier (default: false)
    - `auth_config`: auth server configuration

* Auth configuration

    - `auth_diabled`: flag to disable auth access token (default: False)
    - `auth_host`: auth server host
    - `auth_subdomain`: auth subdomain name
    - `auth_tenant_subdomain`: auth tenant subdomain name


