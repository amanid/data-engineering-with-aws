CREATE EXTERNAL TABLE IF NOT EXISTS step_trainer_landing (
    sensorReadingTime STRING COMMENT 'ISO8601 timestamp when the sensor reading was recorded',
    serialNumber STRING COMMENT 'Unique identifier for the step trainer device',
    distanceFromObject FLOAT COMMENT 'Measured distance from the sensor to an object in centimeters'
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1'
)
LOCATION 's3://nd-parent-bucket/landing/step_trainer_landing/'
TBLPROPERTIES (
    'has_encrypted_data' = 'false',
    'classification' = 'json',
    'compressionType' = 'none'
);
