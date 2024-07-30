### Delta table migration script

This script runs delta table migration sql statements based on provided tenant
configuration.


#### Run

1. Export AWS credential in environment variables

2. Execute the script

```
TRINO_DB_SERVER="<server>" TRINO_DB_USERNAME="<user>" TRINO_DB_PASSWORD="<passsword>" TRINO_DB_CATALOG="delta" bash migration.sh action=create config=config/development/client_config.yaml create_schema=true

options:

action - create/recreate
create_schema - true/false

```
