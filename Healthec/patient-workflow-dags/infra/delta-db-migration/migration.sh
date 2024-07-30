#!/bin/bash

for argument in "$@"
do
   key=$(echo $argument | cut -f1 -d=)

   key_length=${#key}
   value="${argument:$key_length+1}"

   export "$key"="$value"
done

if [[ -z $action ]] ; then
  echo "Error: Action create or recreate should be provided!"
  exit 1
fi
if [[ -z $config ]] ; then
  echo "Error: Tenant configuration should be provided!"
  exit 1
fi
if [[ -z $create_schema ]] ; then
  create_schema=false
fi

# Read trino credentials from environment variables
# trino db server
if [[ -z $TRINO_DB_SERVER ]] ; then
  echo "Error: Trino DB server is missing!"
  exit 1
fi
# trino db username
if [[ -z $TRINO_DB_USERNAME ]] ; then
  echo "Error: Trino DB username is missing!"
  exit 1
fi
# trino db password
if [[ -z $TRINO_DB_PASSWORD ]] ; then
  echo "Error: Trino DB password is missing!"
  exit 1
fi
# trino db catalog
if [[ -z $TRINO_DB_CATALOG ]] ; then
  echo "Error: Trino DB catalog is missing!"
  exit 1
fi

echo "Applying Delta DB migrations with tenant configuration ${config}"
echo "Action: $action"
echo "Create Schema: $create_schema"


function create_schema {
  action=$1
  tenant=$2
  data_s3_bucket=$3
  schema=$4
  # echo "action: $action"
  # echo "tenant: $tenant"
  # echo "data s3 bucket: $data_s3_bucket"
  # echo "schema: $schema"

  # create schema command
  trino_create_schema_cmd="trino --server $TRINO_DB_SERVER --user $TRINO_DB_USERNAME --password $TRINO_DB_PASSWORD\
    --catalog $TRINO_DB_CATALOG --execute \"CREATE SCHEMA IF NOT EXISTS ${tenant}_${schema};\
    WITH (location='s3a://$data_s3_bucket/delta-tables/$tenant/$schema');\""
  # echo "trino_create_schema_cmd: $trino_create_schema_cmd"
  # execute drop schema command
  eval "$trino_create_schema_cmd"
  if [[ $? -ne 0 ]]; then
    echo "failed to create delta schema '${tenant}_${schema}'"
    exit 1
  fi

}


function create_table {
  action=$1
  tenant=$2
  data_s3_bucket=$3
  schema=$4
  entry=$5
  # echo "action: $action"
  # echo "tenant: $tenant"
  # echo "data s3 bucket: $data_s3_bucket"
  # echo "schema: $schema"
  # echo "entry: $entry"

  filename=$(basename "${entry%.*}")
  find_str="create_${schema}_"
  tablename=${filename/$find_str/}
  # echo "table name: $tablename"

  # prepare sql and copy to tempfile
  random_string=`tr -dc 'A-Za-z0-9' </dev/urandom | head -c 16 ; echo ''`;
  tempfile="/tmp/tmp_$random_string.sql";
  # echo "$tempfile"
  sed -e "s/TENANT/$tenant/g;s/DATA_S3_BUCKET/$data_s3_bucket/g" $entry > $tempfile;

  if [[ "$action" == "recreate" ]]; then
    # drop table command
    trino_drop_table_cmd="trino --server $TRINO_DB_SERVER --user $TRINO_DB_USERNAME --password $TRINO_DB_PASSWORD\
      --catalog $TRINO_DB_CATALOG --execute 'DROP TABLE ${tenant}_${schema}.${tablename};'"
    # echo "trino_drop_table_cmd: $trino_drop_table_cmd"
    # execute drop table command
    eval "$trino_drop_table_cmd"
    if [[ $? -ne 0 ]]; then
      echo "failed to drop delta table '${tenant}_${schema}.${tablename}'"
      exit 1
    fi
    remove_s3_dir $data_s3_bucket "delta-tables/$tenant/$schema/$tablename"
  fi

  # create table command
  trino_create_table_cmd="trino --server $TRINO_DB_SERVER --user $TRINO_DB_USERNAME --password $TRINO_DB_PASSWORD\
      --catalog $TRINO_DB_CATALOG --schema $schema --file \"$tempfile\""
  # echo "trino_create_table_cmd: $trino_create_table_cmd"
  # execute create table command
  eval "$trino_create_table_cmd"
  if [[ $? -ne 0 ]]; then
    echo "failed to create delta table '${tenant}_${schema}.${tablename}'"
    exit 1
  fi

  # clean up tempfile
  rm -f "$tempfile"

}


function remove_s3_dir {
  bucket=$1
  path=$2
  rm_cmd="aws s3 rm s3://$bucket/$path --recursive"
  # echo $rm_cmd
  eval "$rm_cmd"
  if [[ $? -ne 0 ]]; then
    echo "failed to remove s3 path s3://$bucket/$path."
    exit 1
  fi
}


# Parse Data S3 bucket, Tenants
data_s3_bucket=$(yq eval '.data_s3_bucket' "$config")
echo "Data S3 bucket: $data_s3_bucket"

tenants=$(yq eval '.tenants | keys | .[]' "$config")
for tenant in $tenants; do
  # echo "Processing tenant: ${tenant}"
  schemas=$(yq eval ".tenants.\"$tenant\" | keys | .[]" "$config")
  for schema in $schemas; do
    # echo "Processing schema: ${schema}"

    # create schema
    if [[ $create_schema == true ]]; then
      create_schema "$action" "$tenant" "$data_s3_bucket" "$schema"
    fi

    formats=$(yq eval ".tenants.\"$tenant\".\"$schema\" | .[]" "$config")
    for format in $formats; do
      # echo "Processing format: ${format}"
      search_dir=`find migrations/${schema^^}/${format} -name *.sql`
      IFS=$'\n' entries=($search_dir)
      for entry in "${entries[@]}"; do
        # create table
        create_table "$action" "$tenant" "$data_s3_bucket" "$schema" "$entry"
      done
    done
  done
done

echo "Delta DB migrations completed successfully."

exit 0
