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

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

customer_trusted_df = glueContext.create_dynamic_frame.from_catalog(
    database="your_glue_db",
    table_name="customer_trusted"
)

accelerometer_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://your-bucket/landing/accelerometer_landing.sql/"]},
    format="json"
)

joined = Join.apply(
    frame1=accelerometer_df,
    frame2=customer_trusted_df,
    keys1=["user"],
    keys2=["email"]
)

fields = ["timeStamp", "user", "x", "y", "z"]
trusted_df = joined.select_fields(fields)

glueContext.write_dynamic_frame.from_options(
    frame=trusted_df,
    connection_type="s3",
    connection_options={"path": "s3://your-bucket/trusted/accelerometer_trusted/"},
    format="json"
)

job.commit()
