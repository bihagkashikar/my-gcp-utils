#!/bin/bash

project=$1
datasetname=$2

for tbl in $(bq query --format=prettyjson --use_legacy_sql=false 'SELECT TABLE_NAME FROM $project.$datasetname.INFORMATION_SCHEMA.TABLES ORDER BY 1' | jq ".[].TABLE_NAME" | sed -e 's/^"//' -e 's/"$//')
do
	echo "generating schema for table " $tbl
	bq show --format=prettyjson $project:datasetname.$tbl | jq '.schema.fields' > $tbl.json
done