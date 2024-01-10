# udp-data-utils

## Load Multiple BigQuery tables from GCS files

Load Multiple BigQuery tables from GCS files script gets a set of csv files from Google Cloud Storage bucket
and loads them into their associate tables in BigQuery.

Reminder to change project id in terminal to the required project id before running script e.g gcloud config set project prj-udp-n-dev-main-mid1

Sample run command:
* ./load_multi_bq_tables_from_gcs.sh prj-udp-n-dev-main-mid1 bq_np_udp_dev_ds_leslie gs://bkt-np-udp-dev-data-leslie/udp-export-from-prenpe-to-npe/*.csv csv 26

 Parameters:
 * Parameter 1 : project name of the target dataset e.g. prj-udp-n-dev-main-mid1
 * Parameter 2 : target dataset e.g. bq_np_udp_dev_ds_leslie
 * Parameter 3 : GCS source bucket path e.g. gs://bkt-np-udp-dev-data-leslie/udp-export-from-prenpe-to-npe/*.csv
 * Parameter 4 : source file extension e.g. csv
 * Parameter 5 : file basename suffix length (that is not part of the table name) e.g. 26 (length for 20230919_part_000000000000) from file name = UNITPRIC20230919_part_000000000000.csv

### Sample scripts/commands used for transfering data from pre-npe environment to npe environment

Files description:
* sample_script_create_tables_from_another_dataset.sql - Sample script to create tables in one dataset based on tables from another dataset
* sample_script_export_data_from_table_to_gcs.sql - Sample script to export data from a set of tables from a particular dataset to GCS 

Run steps:
* Run sample_script_export_data_from_table_to_gcs.sql
* Run sample_cmd_copy_files_from_one_bucket_to_another_bucket.txt
* Run sample_script_create_tables_from_another_dataset.sql
* Run command ./load_multi_bq_tables_from_gcs.sh prj-udp-n-dev-main-mid1 bq_np_udp_dev_ds_leslie gs://bkt-np-udp-dev-data-leslie/udp-export-from-prenpe-to-npe/*.csv csv 26