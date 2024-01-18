#!/bin/bash

# load_multi_bq_tables_from_gcs.sh script gets a set of csv files from Google Cloud Storage bucket
# and loads them into their associate tables in BigQuery.
#
# Parameters
# ----------
# project : project name of the target dataset e.g. prj-dev
# target_dataset : target dataset e.g. #YOUR_DATASET
# src_bkt_path : GCS source bucket path e.g. gs://bkt-bihag-developer/my-dataset-table-export/*.csv
# file_ext : source file extension e.g. csv
# suffix_len : file basename suffix length (that is not part of the table name) e.g. 26 (length for 20230919_part_000000000000) from file name = UNITPRIC20230919_part_000000000000.csv
#
# Run sample:
# Reminder to change project id in terminal to the required project id before running script e.g gcloud config set project #YOUR_PROJECT
# ./load_multi_bq_tables_from_gcs.sh prj-dev bq_dataset_bihag gs://bkt-bihag-developer/my-dataset-table-export/*.csv csv 26

project=$1
target_dataset=$2
src_bkt_path=$3
file_ext=$4
suffix_len=$5

for file_full_path in $(gsutil ls $src_bkt_path)
do
        file_name=$(basename "${file_full_path}" ".$file_ext")
        # removed suffix from file name when required to match table name
        file_name=${file_name:0:${#file_name}-$suffix_len}
        bq_path="$target_dataset.$file_name"
        bq_full_path="$project:$target_dataset.$file_name"

        echo "###########################START####################################"
        echo "Working on the file - $file_name"

        bq load \
        --source_format=CSV \
        --skip_leading_rows=1 \
        --replace=true \
        $bq_path \
        ${file_full_path} \
        | bq show -format=prettyjson $bq_full_path | jq .schema.fields

        echo "$file_name has been added to $bq_full_path!!"
        echo "###########################END######################################"
        echo -e "\n"

done