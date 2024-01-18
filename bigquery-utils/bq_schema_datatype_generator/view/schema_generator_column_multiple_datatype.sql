CREATE OR REPLACE VIEW `data_utilities.schema_generator_column_multiple_datatype` AS
with view_data as (
SELECT
  col.table_catalog, col.table_schema, col.table_name, col.column_name, col.data_type,
  count(distinct col.data_type) OVER (PARTITION BY col.column_name) AS cnt_dt
FROM
  	#YOUR_DATASET.INFORMATION_SCHEMA.TABLES tbl,
    #YOUR_DATASET.INFORMATION_SCHEMA.COLUMNS col
WHERE tbl.table_type = 'VIEW'
AND col.table_catalog = tbl.table_catalog
AND col.table_schema = tbl.table_schema
AND col.table_name = tbl.table_name
UNION ALL
SELECT
  col.table_catalog, col.table_schema, col.table_name, col.column_name, col.data_type,
  count(distinct col.data_type) OVER (PARTITION BY col.column_name) AS cnt_dt
FROM
  	#YOUR_DATASET.INFORMATION_SCHEMA.TABLES tbl,
    #YOUR_DATASET.INFORMATION_SCHEMA.COLUMNS col
WHERE tbl.table_type = 'VIEW'
AND col.table_catalog = tbl.table_catalog
AND col.table_schema = tbl.table_schema
AND col.table_name = tbl.table_name
UNION ALL
SELECT
  col.table_catalog, col.table_schema, col.table_name, col.column_name, col.data_type,
  count(distinct col.data_type) OVER (PARTITION BY col.column_name) AS cnt_dt
FROM
  	#YOUR_DATASET.INFORMATION_SCHEMA.TABLES tbl,
    #YOUR_DATASET.INFORMATION_SCHEMA.COLUMNS col
WHERE tbl.table_type = 'VIEW'
AND col.table_catalog = tbl.table_catalog
AND col.table_schema = tbl.table_schema
AND col.table_name = tbl.table_name
UNION ALL
SELECT
  col.table_catalog, col.table_schema, col.table_name, col.column_name, col.data_type,
  count(distinct col.data_type) OVER (PARTITION BY col.column_name) AS cnt_dt
FROM
  	#YOUR_DATASET.INFORMATION_SCHEMA.TABLES tbl,
    #YOUR_DATASET.INFORMATION_SCHEMA.COLUMNS col
WHERE tbl.table_type = 'VIEW'
AND col.table_catalog = tbl.table_catalog
AND col.table_schema = tbl.table_schema
AND col.table_name = tbl.table_name
UNION ALL
SELECT
  col.table_catalog, col.table_schema, col.table_name, col.column_name, col.data_type,
  count(distinct col.data_type) OVER (PARTITION BY col.column_name) AS cnt_dt
FROM
  	udp_data_raw_outsystems_cdc.INFORMATION_SCHEMA.TABLES tbl,
    udp_data_raw_outsystems_cdc.INFORMATION_SCHEMA.COLUMNS col
WHERE tbl.table_type = 'VIEW'
AND col.table_catalog = tbl.table_catalog
AND col.table_schema = tbl.table_schema
AND col.table_name = tbl.table_name
UNION ALL
SELECT
  col.table_catalog, col.table_schema, col.table_name, col.column_name, col.data_type,
  count(distinct col.data_type) OVER (PARTITION BY col.column_name) AS cnt_dt
FROM
  	udp_data_raw_outsystems_ini.INFORMATION_SCHEMA.TABLES tbl,
    udp_data_raw_outsystems_ini.INFORMATION_SCHEMA.COLUMNS col
WHERE tbl.table_type = 'VIEW'
AND col.table_catalog = tbl.table_catalog
AND col.table_schema = tbl.table_schema
AND col.table_name = tbl.table_name),
table_array as (
SELECT
  table_catalog || "." || table_schema as dataset,
  column_name,
  STRUCT(data_type,
  array_agg(table_name order by table_name) as table_name) AS datatype_struct
FROM view_data
WHERE cnt_dt > 1
GROUP BY table_catalog, table_schema, column_name, data_type),
column_array as (
SELECT
  dataset,
  STRUCT(column_name,
  array_agg(datatype_struct order by datatype_struct.data_type) as datatype_group) AS column_struct
FROM table_array
GROUP BY dataset, column_name)
SELECT
  dataset,
  array_agg(column_struct order by column_struct.column_name) as column_group
FROM column_array
GROUP BY dataset