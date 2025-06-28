"""
This Glue job joins the trusted customer and accelerometer datasets
to identify customers who actually have device data. The output is
a curated list of customers who consented and actively used the product.
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import Join

# Initialize job context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load trusted customer data
customer_df = glueContext.create_dynamic_frame.from_catalog(
    database="my_glue_db",
    table_name="customer_trusted"
)

# Load trusted accelerometer data
accelerometer_df = glueContext.create_dynamic_frame.from_catalog(
    database="my_glue_db",
    table_name="accelerometer_trusted"
)

# Join customers with actual device data (email == user)
curated_df = Join.apply(
    frame1=customer_df,
    frame2=accelerometer_df,
    keys1=["email"],
    keys2=["user"]
).select_fields(["serialNumber", "customerName", "email"])

# Remove duplicates
curated_df = curated_df.drop_duplicates()

# Write to curated zone in S3
glueContext.write_dynamic_frame.from_options(
    frame=curated_df,
    connection_type="s3",
    connection_options={"path": "s3://nd-parent-bucket/curated/customer_curated/"},
    format="json"
)

job.commit()
