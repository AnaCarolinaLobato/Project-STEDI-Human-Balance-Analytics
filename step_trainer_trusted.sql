CREATE EXTERNAL TABLE `step_trainer_trusted`(
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://lake-house-stedi/step_trusted/'
TBLPROPERTIES (
  'CreatedByJob'='step_trusted', 
  'CreatedByJobRun'='jr_9abebbe27749a3e4e42597b2cfed200ce39fc9e1d9e3b73b189219e60f40dc95', 
  'classification'='json')
