#!/usr/bin/env bash

# SCRIPT_DIR=$(cd -- "$(dirname "$0")" > /dev/null 2>&1 ; pwd -P)

# echo "SCRIPT_DIR: ${SCRIPT_DIR}"

MODULES_DIR="python-deps-modules"
MODULES_PKG_FILE="python-deps.zip"

echo "Packaging python dependency modules..."

# remove dependency modules directory if exists and create directory
rm -rf $MODULES_DIR && mkdir $MODULES_DIR

# copy all modules into dependency modules directory
cp -r src/* $MODULES_DIR

# remove jobs module
cd $MODULES_DIR && rm -rf jobs $MODULES_PKG_FILE

# build dependency modules package
zip -9r $MODULES_PKG_FILE . -x \*/__pycache__/\*
cd ..
mkdir -p infra/docker/resources
cp $MODULES_DIR/$MODULES_PKG_FILE infra/docker/resources/$MODULES_PKG_FILE

# clean up
rm -rf $MODULES_DIR

echo "The python dependency modules package has been created and copied to ${MODULES_PKG_FILE}."
