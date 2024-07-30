#!/bin/bash

dest_dir=$DEST_LOCAL_DIR
s3_path=$S3_PATH_SYNC
echo "The source s3 path to sync $s3_path"

while true; do python aws_s3_sync.py --s3-path $s3_path --dest-dir $dest_dir; sleep $REFRESH_INTERVAL; done
