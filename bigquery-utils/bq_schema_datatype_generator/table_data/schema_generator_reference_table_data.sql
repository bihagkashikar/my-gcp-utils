INSERT INTO `data_utilities.schema_generator_reference_table` (
  SELECT "CDC_DATASET_WHERE_CLAUSE_FROM_NUM_DAYS" ,"udp_data_raw_evolve" , "metadata_inserted_timestamp_utc", 7
  UNION ALL
  SELECT "CDC_DATASET_WHERE_CLAUSE_FROM_NUM_DAYS" ,"udp_data_raw_tas" , "metadata_inserted_timestamp_utc", 7
  UNION ALL
  SELECT "CDC_DATASET_WHERE_CLAUSE_FROM_NUM_DAYS" ,"udp_data_raw_outsystems" , "metadata_inserted_timestamp_utc", 7
);