CREATE OR REPLACE PROCEDURE `data_utilities.schema_generator_table_column_match_pattern`(runDatetime DATETIME, workProjectName STRING, workDatasetName STRING, workTargetTableName STRING, sourceProjectName STRING, sourceDatasetName STRING, sourceTableName STRING)
BEGIN
/*
schema_generator_table_column_match_pattern procedure gets each column of a given source table and calls schema_generator_column_match_pattern procedure to analyse the column data value and matches them to a predefined match pattern.

Input parameters:
  runDatetime : Datetime when job has started
work* parameters relate to work (processing) components
  workProjectName : Project name for work components
  workDatasetName : Dataset name for work components
  workTargetTableName : Target table name for work components e.g. schema_generator_match_pattern
source* parameters relate to source components
  sourceProjectName : Project name for source components
  sourceDatasetName : Dataset name for source components
  sourceTableName : Source table name for source components

Sample to call this procedure:
CALL `data_utilities.schema_generator_table_column_match_pattern`(CURRENT_DATETIME, "prj-udp-n-dev-main-mid1","data_utilities","schema_generator_match_pattern","prj-udp-n-dev-main-mid1","udp_data_utils_test_data","schema_generator_test_table_1");
*/
DECLARE queryString STRING; -- build dynamic SQL for execution

/* Creating a temporary work_table so that it can be used with the FOR statement in GoogleSQL procedural language */
SET queryString = """
CREATE TEMP TABLE work_schema_generator_table_column_match_pattern(table_catalog STRING, table_schema STRING, table_name STRING, column_name STRING, ordinal_position NUMERIC, data_type STRING)
AS
SELECT table_catalog, table_schema, table_name, column_name, ordinal_position, data_type
FROM `"""||sourceProjectName||"""."""||sourceDatasetName||""".INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '"""||sourceTableName||"""';
""";
EXECUTE IMMEDIATE queryString;

/* For each column in the table, derive the datatype base on data value */
FOR record IN
(
SELECT table_catalog, table_schema, table_name, column_name, IFNULL(ordinal_position, 99999) as ordinal_position, data_type
FROM work_schema_generator_table_column_match_pattern
)  
DO
  CALL `data_utilities.schema_generator_column_match_pattern`(runDatetime, workProjectName, workDatasetName, workTargetTableName, record.table_catalog, record.table_schema, record.table_name, record.column_name, record.ordinal_position, record.data_type);
END FOR;

DROP TABLE work_schema_generator_table_column_match_pattern;

END;