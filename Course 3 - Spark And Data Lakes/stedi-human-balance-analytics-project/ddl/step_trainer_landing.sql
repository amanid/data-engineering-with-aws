CREATE EXTERNAL TABLE IF NOT EXISTS step_trainer_landing (
    sensorReadingTime STRING,
    serialNumber STRING,
    distanceFromObject FLOAT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1'
)
LOCATION 's3://your-bucket/landing/step_trainer_landing/'
TBLPROPERTIES ('has_encrypted_data'='false');
