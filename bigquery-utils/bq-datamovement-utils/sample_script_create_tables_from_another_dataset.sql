/* Sample script to create tables in one dataset based on tables from another dataset */

BEGIN

DECLARE queryString STRING;

FOR record IN
(
select table_name
from `YOUR_PROJECT.YOUR_DATASET.INFORMATION_SCHEMA.TABLES`
where table_type = 'BASE TABLE'
--and table_name in ('tableA','tableB')
)  
DO

SET queryString = """
CREATE OR REPLACE TABLE `YOUR_PROJECT.YOUR_DATASET."""||record.table_name||"""` LIKE `YOUR_PROJECT.YOUR_DATASET."""||record.table_name||"""`
""";
EXECUTE IMMEDIATE queryString;

END FOR;

END;
