CREATE OR REPLACE PROCEDURE `udp_utilities.schema_generator_column_match_pattern`(runDatetime DATETIME, workProjectName STRING, workDatasetName STRING, workTargetTableName STRING, sourceProjectName STRING, sourceDatasetName STRING, sourceTableName STRING, sourceColumnName STRING, sourceColumnPosition NUMERIC, sourceColumnDatatype STRING)
BEGIN
/*
schema_generator_column_match_pattern procedure analyses a column data value and matches them to a predefined match pattern.
The match pattern is stored in schema_generator_match_pattern table.

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
  sourceColumnName : Source column name for source components
  sourceColumnPosition : Source column position for source components
  sourceColumnDatatype : Source column datatype for source components    

Sample to call this procedure:
CALL `udp_utilities.schema_generator_column_match_pattern`(CURRENT_DATETIME, "prj-udp-n-dev-main-mid1","udp_utilities","schema_generator_match_pattern","prj-udp-n-dev-main-mid1","udp_data_utils_test_data","schema_generator_test_table_1","number_column",1,"STRING");
*/
DECLARE queryString STRING; -- build dynamic SQL for execution

/* if the sourceColumnDatatype is not STRING, then writes 'NOT_MATCH' results to a table */
IF sourceColumnDatatype != 'STRING' THEN
SET queryString = """
INSERT INTO `"""||workProjectName||"""."""||workDatasetName||"""."""||workTargetTableName||"""`
  (rundatetime, projectname, datasetname, tablename, columnname, columnposition, columndatatype, columndatatypematch)
  VALUES (DATETIME('"""||runDatetime||"""'), '"""||sourceProjectName||"""', '"""||sourceDatasetName||"""', '"""||sourceTableName||"""',
  '"""||sourceColumnName||"""', """||sourceColumnPosition||""", '"""||sourceColumnDatatype||"""', 'NOT_MATCH')
""";
ELSE
/*
Use case statement rules to determine match pattern base on data value and writes the results to a table.
SQL below will handle null data value and also no data in the source table.
*/
SET queryString = """
INSERT INTO `"""||workProjectName||"""."""||workDatasetName||"""."""||workTargetTableName||"""`
  (rundatetime, projectname, datasetname, tablename, columnname, columnposition, columndatatype, columndatatypematch)
WITH TI AS (
SELECT
DATETIME('"""||runDatetime||"""') as rundatetime,
'"""||sourceProjectName||"""' as projectname,
'"""||sourceDatasetName||"""' as datasetname,
'"""||sourceTableName||"""' as tablename,
'"""||sourceColumnName||"""' as columnname,
"""||sourceColumnPosition||""" as columnposition,
'"""||sourceColumnDatatype||"""' as columndatatype),
TA AS (
SELECT DISTINCT
'"""||sourceProjectName||"""' as projectname,
'"""||sourceDatasetName||"""' as datasetname,
'"""||sourceTableName||"""' as tablename,
'"""||sourceColumnName||"""' as columnname,
case
  when regexp_contains("""||sourceColumnName||""", r'^\\d{4}-(?:[1-9]|0[1-9]|1[012])-(?:[1-9]|0[1-9]|[12][0-9]|3[01])$') then 'DATE_MATCHER'
  when lower("""||sourceColumnName||""") in ('true','false') then 'BOOLEAN_MATCHER_DEFAULT'
  when regexp_contains("""||sourceColumnName||""", r'^\\d{1,2}:\\d{1,2}:\\d{1,2}(\\.\\d{1,3})\\+\\d{1,4}?$') then 'EVOLVE_TIME_MATCHER'
  when regexp_contains("""||sourceColumnName||""", r'^\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{1,2}:\\d{1,2}(\\.\\d{1,3})[+-]\\d{4}?$') then 'EVOLVE_DATETIME_MATCHER'
  when regexp_contains("""||sourceColumnName||""", r'^\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{1,2}:\\d{1,2}(\\.\\d{1,6})[+-]\\d{1,2}:\\d{1,2}?$') then 'EVOLVE_DATETIME_MATCHER_2'
  when regexp_contains("""||sourceColumnName||""", r'^\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{1,2}:\\d{1,2}[+-]\\d{4}?$') then 'EVOLVE_DATETIME_MATCHER_3'
  when regexp_contains("""||sourceColumnName||""", r'^\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{1,2}:\\d{1,2}[+-]\\d{1,2}:\\d{1,2}?$') then 'EVOLVE_DATETIME_MATCHER_4'  
  when regexp_contains("""||sourceColumnName||""", r'^\\d{4}-\\d{1,2}-\\d{1,2}-\\d{1,2}.\\d{1,2}.\\d{1,2}(\\.\\d{1,6})$') then 'TAS_DATETIME_MATCHER'  
  when regexp_contains("""||sourceColumnName||""", r'^[-+]?\\d+$') then 'INTEGER_MATCHER'
  when regexp_contains("""||sourceColumnName||""", r'^[-+]?(?:\\d+\\.?\\d*|\\.\\d+)(?:[eE][-+]?\\d+)?$') then 'FLOAT_MATCHER'
  else 'NO_MATCHER'
end as columndatatypematch
FROM `"""||sourceProjectName||"""."""||sourceDatasetName||"""."""||sourceTableName||"""`
WHERE """||sourceColumnName||""" IS NOT NULL
)
SELECT TI.rundatetime, TI.projectname, TI.datasetname, TI.tablename, TI.columnname, TI.columnposition, TI.columndatatype, IFNULL(TA.columndatatypematch,"NO_MATCHER")
FROM TI LEFT JOIN TA
ON TI.projectname = TA.projectname
AND TI.datasetname = TA.datasetname
AND TI.tablename = TA.tablename
AND TI.columnname = TA.columnname
""";
END IF;
EXECUTE IMMEDIATE queryString;

END;