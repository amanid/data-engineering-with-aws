CREATE EXTERNAL TABLE IF NOT EXISTS customer_landing (
    serialNumber STRING,
    shareWithPublicAsOfDate STRING,
    birthday STRING,
    registrationDate STRING,
    shareWithResearchAsOfDate STRING,
    customerName STRING,
    email STRING,
    lastUpdateDate STRING,
    phone STRING,
    shareWithFriendsAsOfDate STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1'
)
LOCATION 's3://your-bucket/landing/customer_landing/'
TBLPROPERTIES ('has_encrypted_data'='false');
