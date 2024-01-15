CREATE OR REPLACE VIEW `data_utilities.schema_generator_match_datatype` AS
SELECT rundatetime, projectname, datasetname, tablename, columnname, columnposition, columndatatype,
string_agg(columndatatypematch, ", " order by columndatatypematch) as columndatatypematchall,
/* if the columndatatype is not STRING, the return the columndatatype value, else derive the datatype */
case string_agg(columndatatypematch, ", " order by columndatatypematch)
when 'BOOLEAN_MATCHER_DEFAULT' then 'BOOLEAN'
when 'DATE_MATCHER' then 'DATE'
when 'EVOLVE_DATETIME_MATCHER' then 'TIMESTAMP'
when 'EVOLVE_DATETIME_MATCHER_2' then 'TIMESTAMP'
when 'EVOLVE_DATETIME_MATCHER_3' then 'TIMESTAMP'
when 'EVOLVE_DATETIME_MATCHER_4' then 'TIMESTAMP'
when 'EVOLVE_DATETIME_MATCHER, EVOLVE_DATETIME_MATCHER_3' then 'TIMESTAMP'
when 'EVOLVE_DATETIME_MATCHER_2, EVOLVE_DATETIME_MATCHER_4' then 'TIMESTAMP'
when 'TAS_DATETIME_MATCHER' then 'TIMESTAMP'
when 'EVOLVE_TIME_MATCHER' then 'TIME'
when 'FLOAT_MATCHER' then 'NUMERIC'
when 'INTEGER_MATCHER' then 'NUMERIC'
when 'FLOAT_MATCHER, INTEGER_MATCHER' then 'NUMERIC'
when 'NO_MATCHER' then 'STRING'
when 'NOT_MATCH' then columndatatype
else 'STRING'
end as columndatatyperesult
FROM `data_utilities.schema_generator_match_pattern`
group by rundatetime, projectname, datasetname, tablename, columnname, columnposition, columndatatype;