CREATE OR REPLACE PROCEDURE `data_utilities.schema_generator_run`(processFromSourceDataset STRING, workProjectName STRING, workDatasetName STRING, workTargetTableName1 STRING, workTargetTableName2 STRING, workTargetTableName3 STRING, workTargetTableName4 STRING, workTargetTableName5 STRING, workTargetTableName6 STRING, sourceProjectName STRING, sourceDatasetName STRING, sourceDatasetCdcName STRING, sourceDatasetIniName STRING)
BEGIN
/*
schema_generator_run procedure gets each table of a given dataset/given table list and calls schema_generator_table_column_match and schema_generator_create_view procedures.

Input parameters:
  processFromSourceDataset : If "Y" then process all tables in sourceDatasetName dataset
work* parameters relate to work (processing) components
  workProjectName : Project name for work components
  workDatasetName : Dataset name for work components
  workTargetTableName1 : Target table name for work components e.g. schema_generator_run_log
  workTargetTableName2 : Target table name for work components e.g. schema_generator_match_pattern
  workTargetTableName3 : Target table name for work components e.g. schema_generator_match_datatype
  workTargetTableName4 : Target table name for work components e.g. schema_generator_result
  workTargetTableName5 : Target table name for work components e.g. schema_generator_table
  workTargetTableName6 : Target table name for work components e.g. schema_generator_reference_table  
source* parameters relate to source components
  sourceProjectName : Project name for source components
  sourceDatasetName : Dataset name for source components
  sourceDatasetCdcName : Dataset CDC name for source components
  sourceDatasetIniName : Dataset INI name for source components

Sample to call this procedure:
CALL `data_utilities.schema_generator_run`("Y","prj-udp-n-dev-main-mid1","data_utilities","schema_generator_run_log","schema_generator_match_pattern","schema_generator_match_datatype","schema_generator_result","schema_generator_table","schema_generator_reference_table","prj-udp-n-dev-main-mid1","udp_data_utils_test_data","udp_data_utils_test_data_cdc","udp_data_utils_test_data_ini");
*/
DECLARE queryString STRING; -- build dynamic SQL for execution
DECLARE runDatetime DATETIME; -- run datetime

SET runDatetime = CURRENT_DATETIME;

IF processFromSourceDataset = 'Y' THEN
/* Creating a temporary work_table so that it can be used with the FOR statement in GoogleSQL procedural language */
SET queryString = """
CREATE TEMP TABLE work_schema_generator_run(table_catalog STRING, table_schema STRING, table_name STRING)
AS
SELECT table_catalog, table_schema, table_name
FROM `"""||sourceProjectName||"""."""||sourceDatasetName||""".INFORMATION_SCHEMA.TABLES`
WHERE table_type = 'BASE TABLE';
""";
ELSE
/* Creating a temporary work_table so that it can be used with the FOR statement in GoogleSQL procedural language */
SET queryString = """
CREATE TEMP TABLE work_schema_generator_run(table_catalog STRING, table_schema STRING, table_name STRING)
AS
SELECT projectname, datasetname, tablename
FROM `"""||workProjectName||"""."""||workDatasetName||"""."""||workTargetTableName5||"""`;
""";
END IF;
EXECUTE IMMEDIATE queryString;

FOR record IN
(
SELECT table_catalog, table_schema, table_name
FROM work_schema_generator_run
ORDER BY table_name
)  
DO
/* Insert record into schema_generator_run_log table */
SET queryString = """
INSERT INTO `"""||workProjectName||"""."""||workDatasetName||"""."""||workTargetTableName1||"""`
  (rundatetime, projectname, datasetname, tablename)
  VALUES ('"""||runDatetime||"""', '"""||record.table_catalog||"""', '"""||record.table_schema||"""', '"""||record.table_name||"""')
""";
EXECUTE IMMEDIATE queryString;

/* Calls schema_generator_table_column_match_pattern procedure */
CALL `data_utilities.schema_generator_table_column_match_pattern`(runDatetime,workProjectName,workDatasetName,workTargetTableName2,record.table_catalog,record.table_schema,record.table_name);

/* Calls schema_generator_create_view procedure */
CALL `data_utilities.schema_generator_create_view`(runDatetime,workProjectName,workDatasetName,workTargetTableName3,workTargetTableName6,workTargetTableName4,record.table_catalog,record.table_schema,record.table_name,sourceDatasetCdcName,sourceDatasetIniName);

END FOR;

DROP TABLE work_schema_generator_run;

END;