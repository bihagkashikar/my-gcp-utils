/* Sample script to export data from a set of tables from a particular dataset to GCS */

BEGIN

DECLARE queryString STRING;

FOR record IN
(
select table_name
from `YOUR_PROJECT.YOUR_DATASET.INFORMATION_SCHEMA.TABLES`
where table_type = 'BASE TABLE'
and table_name in ('tta344','tta1374','UNITPRIC','tta225','tta226','tta394','tta287','tmi001_Company','tmi002_company')
)  
DO

SET queryString = """
EXPORT DATA
  OPTIONS ( uri = 'gs://YOUR_GCS/"""||record.table_name||"""'||CAST(CURRENT_DATE() AS STRING FORMAT 'YYYYMMDD')||"_part_"||'*.csv',
    format = 'CSV',
    OVERWRITE = TRUE,
    header = TRUE,
    field_delimiter = ',') AS (
  SELECT
    *
  FROM
    `YOUR_PROJECT.YOUR_DATASET."""||record.table_name||"""`
  ORDER BY
    metadata_inserted_datetime_utc )
""";
EXECUTE IMMEDIATE queryString;

END FOR;

END;