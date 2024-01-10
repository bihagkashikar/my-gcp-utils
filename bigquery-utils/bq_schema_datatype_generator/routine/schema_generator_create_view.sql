CREATE OR REPLACE PROCEDURE `udp_utilities.schema_generator_create_view`(runDatetime DATETIME, workProjectName STRING, workDatasetName STRING, workSourceTableName STRING, workReferenceTableName STRING, workTargetTableName STRING, sourceProjectName STRING, sourceDatasetName STRING, sourceTableName STRING, sourceDatasetCdcName STRING, sourceDatasetIniName STRING)
BEGIN
/*
schema_generator_create_view procedure dynamically creates a view for a given source table.
The created view has the same columns as its source table but the datatype of the columns may be converted to another datatype based on the column's data value.
The datatype conversion rule is defined in the schema_generator_match_datatype view.
The sql to create the view is also stored in schema_generator_result table.

Input parameters:
  runDatetime : Datetime when job has started
work* parameters relate to work (processing) components
  workProjectName : Project name for work components
  workDatasetName : Dataset name for work components
  workSourceTableName : Source table (or view) name for work components e.g. schema_generator_match_datatype
  workReferenceTableName : Reference table name for work components e.g. schema_generator_reference_table
  workTargetTableName : Target table name for work components e.g. schema_generator_result
source* parameters relate to source components
  sourceProjectName : Project name for source components
  sourceDatasetName : Dataset name for source components
  sourceTableName : Source table name for source components
  sourceDatasetCdcName : Dataset CDC name for source components
  sourceDatasetIniName : Dataset INI name for source components

Sample to call this procedure:
CALL `udp_utilities.schema_generator_create_view`(CURRENT_DATETIME, "prj-udp-n-dev-main-mid1","udp_utilities","schema_generator_match_datatype","schema_generator_reference_table","schema_generator_result","prj-udp-n-dev-main-mid1","udp_data_utils_test_data","schema_generator_test_table_1","udp_data_utils_test_data_cdc","udp_data_utils_test_data_ini");
*/
DECLARE queryHeadStringCdc STRING; -- for building dynamic SQL for execution
DECLARE queryHeadStringIni STRING; -- for building dynamic SQL for execution
DECLARE queryBodyString STRING; -- for building dynamic SQL for execution
DECLARE queryTailStringCdc STRING; -- for building dynamic SQL for execution
DECLARE queryTailStringIni STRING; -- for building dynamic SQL for execution
DECLARE queryColumn STRING; -- for building dynamic SQL for execution
DECLARE queryString STRING; -- for building dynamic SQL for execution

/* Get partition column */
SET queryString = """SELECT IFNULL(referencesubcategory,default_column) AS partition_column FROM (SELECT \"metadata_inserted_timestamp_utc\" as default_column ) LEFT JOIN (SELECT referencesubcategory FROM `"""||workProjectName||"""."""||workDatasetName||"""."""||workReferenceTableName||"""` WHERE referencetype = \"CDC_DATASET_WHERE_CLAUSE_FROM_NUM_DAYS\" AND referencecategory = \""""||sourceDatasetName||"""\") ON TRUE""";
EXECUTE IMMEDIATE queryString INTO queryColumn;

/* Query components to generate SQL for creating view */
SET queryHeadStringCdc = """CREATE OR REPLACE VIEW `"""||sourceDatasetCdcName||"""."""||sourceTableName||"""_vw` AS WITH RD AS (SELECT CAST(IFNULL(referencedata,default_days) AS INT64) AS from_num_days FROM (SELECT -1 as default_days ) LEFT JOIN (SELECT referencedata FROM `"""||workDatasetName||"""."""||workReferenceTableName||"""` WHERE referencetype = \"CDC_DATASET_WHERE_CLAUSE_FROM_NUM_DAYS\" AND referencecategory = \""""||sourceDatasetName||"""\") ON TRUE) SELECT""";
SET queryHeadStringIni = """CREATE OR REPLACE VIEW `"""||sourceDatasetIniName||"""."""||sourceTableName||"""_vw` AS SELECT""";
SET queryBodyString = "";
SET queryTailStringCdc = """FROM `"""||sourceDatasetName||"""."""||sourceTableName||"""`, RD WHERE (RD.from_num_days = -1) OR ( """||queryColumn||""" BETWEEN TIMESTAMP_TRUNC(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL RD.from_num_days * 24 HOUR), DAY) AND TIMESTAMP_TRUNC(CURRENT_TIMESTAMP, SECOND));""";
SET queryTailStringIni = """FROM `"""||sourceDatasetName||"""."""||sourceTableName||"""` WHERE FALSE;""";

/* Creating a temporary work_table so that it can be used with the FOR statement in GoogleSQL procedural language */
SET queryString = """
CREATE TEMP TABLE work_schema_generator_create_view(columnposition NUMERIC, columnname STRING, columndatatypematchall STRING, columndatatyperesult STRING)
AS
SELECT columnposition, columnname, columndatatypematchall, columndatatyperesult
FROM `"""||workProjectName||"""."""||workDatasetName||"""."""||workSourceTableName||"""`
where rundatetime = DATETIME('"""||runDatetime||"""') 
and projectname = '"""||sourceProjectName||"""'
and datasetname = '"""||sourceDatasetName||"""'
and tablename = '"""||sourceTableName||"""';
""";
EXECUTE IMMEDIATE queryString;

/* Build body of SQL for creating view */
FOR record IN
(
SELECT 
if (columnposition = 1, "", ", ") || "CAST(" || if(columndatatypematchall='TAS_DATETIME_MATCHER',"CONCAT(LEFT(" || columnname || ",10),\" \",REPLACE(SUBSTR(" || columnname || ",12,8),\".\",\":\"),RIGHT(" || columnname || ",7))",columnname) || " as " || columndatatyperesult || ") as " || columnname as columnline
FROM work_schema_generator_create_view
WHERE columnname NOT IN ('_PARTITIONTIME','_PARTITIONDATE')
order by columnposition
)  
DO
  SET queryBodyString = queryBodyString || record.columnline;
END FOR;

DROP TABLE work_schema_generator_create_view;

/* Insert generated SQL into a table */
SET queryString = """
INSERT INTO `"""||workProjectName||"""."""||workDatasetName||"""."""||workTargetTableName||"""`
  (rundatetime, projectname, datasetname, tablename, sql_cdc, sql_ini)
  VALUES (DATETIME('"""||runDatetime||"""'), '"""||sourceProjectName||"""', '"""||sourceDatasetName||"""', '"""||sourceTableName||"""', '"""||queryHeadStringCdc||""" """||queryBodyString||""" """||queryTailStringCdc||"""', '"""||queryHeadStringIni||""" """||queryBodyString||""" """||queryTailStringIni||"""')
""";
EXECUTE IMMEDIATE queryString;

/* Create view for source table but this is not required
SET queryString = """"""||queryHeadStringCdc||""" """||queryBodyString||""" """||queryTailStringCdc||"""""";
EXECUTE IMMEDIATE queryString;

SET queryString = """"""||queryHeadStringIni||""" """||queryBodyString||""" """||queryTailStringIni||"""""";
EXECUTE IMMEDIATE queryString;
*/

END;