# schema-datatype-generator

## Background
This utility is inspired from https://github.com/bxparks/bigquery-schema-generator, however with limited to only detecting the column data type all built using BigQuery stored procedures. Here are some points on how my utility is different to the one above. 
   * Primary motivation behind building was to have the execution run serverless.
   * Being serverless, It is hosted on BigQuery and hence written in commonly known language in SQL.
   * The utility logs the source data type in source table, and corresponds it with best-fit data type identified by the utility.
   * The utility also generates a VIEW with referenced to the source table, with CAST function converting the data type identified by the utility.
   
## HOWTO use schema data type generator

### Pre-requisites
* The utility REQUIRES all table columns to be in STRING data type loaded in BigQuery Dataset.
* A dataset already created in BigQuery to deploy the stored procedures.

### HOW it works

Schema data type generator creates DDL for views of tables in a given dataset/given table list. These views will have the same columns as their associate tables.

### Steps to run the utility

To use the schema data type generator, run the following statement in BigQuery: 

* To process all tables from a given dataset:
    * CALL `data_utilities.schema_generator_run`("Y","***enter project name for data_utilities dataset***","data_utilities","schema_generator_run_log","schema_generator_match_pattern","schema_generator_match_datatype","schema_generator_result","schema_generator_table","schema_generator_reference_table","***enter project name for dataset containing tables to create views***","***enter dataset name containing tables to create views***","***enter dataset cdc name***","***enter dataset ini name***");
      
* To process all tables from schema_generator_table table, please update this table with the required table data:
    * CALL `data_utilities.schema_generator_run`("N","***enter project name for data_utilities dataset***","data_utilities","schema_generator_run_log","schema_generator_match_pattern","schema_generator_match_datatype","schema_generator_result","schema_generator_table","schema_generator_reference_table","***enter project name for dataset containing tables to create views***","***enter dataset name containing tables to create views***","***enter dataset cdc name***","***enter dataset ini name***");
      
---
### schema data type generator Components

schema_generator\routine folder:

* schema_generator_column_match_pattern.sql - Maps string data values to match pattern types.
* schema_generator_create_view.sql - Generates DDL scripts to create view.
* schema_generator_run.sql - Starts the process to create views for tables in a given dataset.
* schema_generator_table_column_match_pattern.sql - Gets column details of a table for processing.

schema_generator\table folder :

* schema_generator_match_pattern.sql - Stores match pattern types data.
* schema_generator_reference_table.sql - Stores reference data.
* schema_generator_result.sql - Stores DDL scripts to create view.
* schema_generator_run_log.sql - Stores schema data type generator run logs.
* schema_generator_table.sql - Stores list of tables for processing.
* schema_generator_test_table_1.sql - Test table #1.
* schema_generator_test_table_2.sql - Test table #2.
* schema_generator_test_table_3.sql - Test table #3.

schema_generator\table_data folder:

* schema_generator_reference_table_data.sql - Reference data.
* schema_generator_table_data.sql - Test data for list of tables for processing.
* schema_generator_test_table_1_data.sql - Test data for test table #1
* schema_generator_test_table_2_data.sql - Test data for test table #2.
* schema_generator_test_table_3_data.sql - Test data for test table #3.

schema_generator\view folder:

* schema_generator_column_multiple_datatype.sql - Shows same column name with multiple datatypes.
* schema_generator_match_datatype.sql - Maps match pattern types to BigQuery datatypes.

---
### schema data type generator Setup

**Setup steps:**

* Create tables (schema_generator\table folder):
    * Run schema_generator_match_pattern.sql
    * Run schema_generator_result.sql
    * Run schema_generator_run_log.sql
    * Run schema_generator_table.sql
    * Run schema_generator_reference_table.sql
* Create view (schema_generator\view folder):
    * Run schema_generator_match_datatype.sql
    * Run schema_generator_column_multiple_datatype.sql
* Create routines (schema_generator\routine folder):
    * Run schema_generator_column_match_pattern.sql
    * Run schema_generator_table_column_match_pattern.sql
    * Run schema_generator_create_view.sql
    * Run schema_generator_run.sql
* Insert data (schema_generator\table_data folder):
    * Run schema_generator_reference_table_data.sql

---
### Rules

**Type Inference Rules:**

* INTEGER can upgrade to FLOAT
    * if a field in an early record is an INTEGER, but a subsequent record shows this field to have a FLOAT value, the type of the field will be upgraded to a FLOAT
    * the reverse does not happen, once a field is a FLOAT, it will remain a FLOAT
* FLOAT type will be converted to NUMERIC type in the transform
* Conflicting TIME, DATE, TIMESTAMP types upgrades to STRING
  * If a field is determined to have one type of "time" in one record, then subsequently a different "time" type, then the field will be assigned a STRING type
* BOOLEAN, INTEGER, and FLOAT can appear inside quoted strings
  * In other words, "true" (or "True" or "false", etc) is considered a BOOLEAN type, "1" is considered an INTEGER type, and "2.1" is considered a FLOAT type.

---

### BOOLEAN Type Handling

By default, the schema data type generator service library uses a fix set of values to determine whether the column is BOOLEAN type or not

```
The fix set of value is (the schema data type generator service library will automatically lower the case):
* true
* false
```

For instance, a table below with different kinds of potential boolean values:

boolean_column_1|boolean_column_2|boolean_column_3|boolen_column_4
----------------|----------------|----------------|---------------
false|n|f|yes
false|n|t|no
true|n|f|no
true|y|t|yes
true|y|f|yes
false|y|t|no

The schema data type generator service will infer the column type as below:

Column name|Inferred type
-----------|-------
boolean_column_1|BOOLEAN
boolean_column_2|STRING
boolean_column_3|STRING
boolean_column_4|STRING

---

### DATE Type Handling

By default, the schema data type generator service library uses a fix date format to determine whether the column is DATE type or not
```
The fix format is yyyy-[m]m-[d]d
```
For instance, a table below with different date format:

date_column_1| date_column_2 |date_column_3|date_column_4
-------------|--------------|-------------|-------------
2020-09-08|2020-09-08|31/05/2021|10-10-2010
1991-09-09|1991-09-09|31/12/2000|03-03-2002
2003-11-11|2003/11/11|10/10/2021|09-09-2009
2022-12-12|2022/12/12|30/04/2022|2012-12-12
2021-06-25|2021-06-25|25/06/2021|2021-06-25

The schema data type generator service will infer the column type as below:

Column name|Inferred type
-----------|-------
date_column_1|DATE
date_column_2|STRING
date_column_3|STRING
date_column_4|STRING

---

### DATETIME/TIMESTAMP Type Handling

**Data Source Dependent Handling strategy**

Data Type Inference Regex is matched from Data Source Metadata / Data Type Mapping table (https://hq.ioof.com.au/display/UDP/UDP+Technical+Design+-+BigQuery+Data+Types+for+Raw+Structured%2C+Raw+Vault+and+Business+Vault)

For instance, Evolve date-time Regex is
```
r'^\d{4}-\d{1,2}-\d{1,2}T\d{1,2}:\d{1,2}:\d{1,2}(\.\d{1,3})[+-]\d{4}?$'
```

Alternate instance, Evolve date-time Regex is
```
r'^\d{4}-\d{1,2}-\d{1,2}T\d{1,2}:\d{1,2}:\d{1,2}[+-]\d{4}?$'
```

Alternate instance, Evolve date-time Regex is
```
r'^\d{4}-\d{1,2}-\d{1,2}T\d{1,2}:\d{1,2}:\d{1,2}(\.\d{1,6})[+-]\d{1,2}:\d{1,2}?$'
```

Alternate instance, Evolve date-time Regex is
```
r'^\d{4}-\d{1,2}-\d{1,2}T\d{1,2}:\d{1,2}:\d{1,2}[+-]\d{1,2}:\d{1,2}?$'
```

Alternate instance, TAS date-time Regex is
```
r'^\d{4}-\d{1,2}-\d{1,2}-\d{1,2}.\d{1,2}.\d{1,2}(\.\d{1,6})$')
```

Given the table with different date-time format:

datetime_column_1|datetime_column_2|datetime_column_3|datetime_column_4|datetime_column_5|datetime_column_6
-----------------|-----------------|-----------------|-----------------|-----------------|-----------------
2023-06-27T16:22:09.160+1000|2023-06-27 16:22:09.160000 UTC|2023-06-27T16:22:09.160000+10:00|2004-11-08-13.03.30.123694|2023-06-27T16:22:09+1000|2023-06-27T16:22:09+10:00

The schema data type generator service will infer the column type as below:

Column name|Inferred type
-----------|-------
date_column_1|EVOLVE_DATETIME
date_column_2|STRING
date_column_3|EVOLVE_DATETIME
date_column_4|TAS_DATETIME
date_column_5|EVOLVE_DATETIME
date_column_6|EVOLVE_DATETIME

According to the Data Type Mapping table, the date-time column will be converted to TIMESTAMP (UTC as timezone) format

Column name| Raw Structure Value (STRING)     | Transformed Value (TIMESTAMP)
-----------|----------------------------------|-------------------
date_column_1| 2023-06-27T**16**:22:09.160+1000 | 2023-06-27 **06**:22:09.160000 UTC

---

### TIME Type Handling

**Data Source Dependent Handling strategy**

Data Type Inference Regex is matched from Data Source Metadata / Data Type Mapping table (https://hq.ioof.com.au/display/UDP/UDP+Technical+Design+-+BigQuery+Data+Types+for+Raw+Structured%2C+Raw+Vault+and+Business+Vault)

For instance, Evolve time Regex is
```
r'^\d{1,2}:\d{1,2}:\d{1,2}(\.\d{1,3})\+\d{1,4}?$'
```

Given the table with different time format:

time_column_1|time_column_2
-----------------|-----------------
16:22:09.160+1000|16:22:09.160000

The schema data type generator service will infer the column type as below:

Column name|Inferred type
-----------|-------
time_column_1|EVOLVE_TIME
time_column_2|STRING

Sample time format transform is shown below:

Column name| Raw Structure Value (STRING)     | Transformed Value (TIME)
-----------|----------------------------------|-------------------
time_column_1| 16:22:09.160+1000 | 16:22:09.160000
