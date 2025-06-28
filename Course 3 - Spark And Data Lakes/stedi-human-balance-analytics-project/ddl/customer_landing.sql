CREATE EXTERNAL TABLE IF NOT EXISTS customer_landing (
    serialNumber STRING COMMENT 'Serial number of the purchased Step Trainer device',
    shareWithPublicAsOfDate STRING COMMENT 'Date the customer consented to public sharing',
    birthday STRING COMMENT 'Customer date of birth (YYYY-MM-DD)',
    registrationDate STRING COMMENT 'Date the customer registered',
    shareWithResearchAsOfDate STRING COMMENT 'Date the customer consented to share data for research',
    customerName STRING COMMENT 'Full name of the customer',
    email STRING COMMENT 'Customer email address',
    lastUpdateDate STRING COMMENT 'Date the customer record was last updated',
    phone STRING COMMENT 'Customer contact number',
    shareWithFriendsAsOfDate STRING COMMENT 'Date the customer consented to sharing with friends'
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1'
)
LOCATION 's3://nd-parent-bucket/landing/customer_landing/'
TBLPROPERTIES (
    'has_encrypted_data' = 'false',
    'classification' = 'json',
    'compressionType' = 'none'
);
