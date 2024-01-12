# Usage: 
# Replace the project and dataset if you want to generate schema for specific tables
# The utility generates the files locally on developers VDI and the file names are named as name of the table.

project=$1
dataset=$2

for tbl in $(bq query --format=prettyjson --use_legacy_sql=false 'SELECT lower(TABLE_NAME) as TABLE_NAME FROM '$project.$dataset'.INFORMATION_SCHEMA.COLUMNS WHERE table_schema = "'$dataset'" GROUP BY TABLE_NAME ORDER BY TABLE_NAME' | jq ".[].TABLE_NAME" | sed -e 's/^"//' -e 's/"$//')
do
	echo "generating schema for table " $tbl
	echo "bq query --format=prettyjson --use_legacy_sql=false 'SELECT lower(TABLE_NAME) as TABLE_NAME FROM '$project.$dataset'.INFORMATION_SCHEMA.COLUMNS WHERE table_schema = "'$dataset'" GROUP BY TABLE_NAME ORDER BY TABLE_NAME' | jq ".[].TABLE_NAME" | sed -e 's/^"//' -e 's/"$//'"
	#bq query --format=prettyjson --use_legacy_sql=false 'SELECT ARRAY_AGG(STRUCT( description, lower(column_name) as name, data_type as type)) as columns FROM '$project.$dataset'.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS  WHERE table_name = "'$tbl'"' | jq ".[].columns" > $tbl.json
done
