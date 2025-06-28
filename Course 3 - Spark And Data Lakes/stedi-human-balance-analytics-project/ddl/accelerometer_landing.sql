CREATE EXTERNAL TABLE IF NOT EXISTS accelerometer_landing (
    timeStamp STRING COMMENT 'Timestamp of the accelerometer reading in ISO8601 format',
    user STRING COMMENT 'Email or user identifier matching the customer',
    x FLOAT COMMENT 'X-axis accelerometer reading',
    y FLOAT COMMENT 'Y-axis accelerometer reading',
    z FLOAT COMMENT 'Z-axis accelerometer reading'
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1'
)
LOCATION 's3://nd-parent-bucket/landing/accelerometer_landing/'
TBLPROPERTIES (
    'has_encrypted_data' = 'false',
    'classification' = 'json',
    'compressionType' = 'none'
);
