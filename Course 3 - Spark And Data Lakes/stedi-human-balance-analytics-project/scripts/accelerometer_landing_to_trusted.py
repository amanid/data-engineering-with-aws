"""
This Glue job filters accelerometer data by joining it with the trusted customer list,
ensuring only data from consenting users is retained.
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import Join

# Initialize job and context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load trusted customer data from Glue Catalog
customer_trusted_df = glueContext.create_dynamic_frame.from_catalog(
    database="my_glue_db",
    table_name="customer_trusted"
)

# Load raw accelerometer data from S3 landing zone
accelerometer_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://nd-parent-bucket/landing/accelerometer_landing/"]},
    format="json"
)

# Inner join on user (accelerometer) = email (customer)
joined = Join.apply(
    frame1=accelerometer_df,
    frame2=customer_trusted_df,
    keys1=["user"],
    keys2=["email"]
)

# Select only accelerometer fields for output
trusted_df = joined.select_fields(["timeStamp", "user", "x", "y", "z"])

# Write to trusted zone in S3
glueContext.write_dynamic_frame.from_options(
    frame=trusted_df,
    connection_type="s3",
    connection_options={"path": "s3://nd-parent-bucket/trusted/accelerometer_trusted/"},
    format="json"
)

job.commit()
