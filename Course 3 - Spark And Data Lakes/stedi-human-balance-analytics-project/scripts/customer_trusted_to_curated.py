"""
This job joins the trusted customer and accelerometer tables
and filters customers who actually have data in both.
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import Join

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

customer_df = glueContext.create_dynamic_frame.from_catalog(
    database="your_glue_db",
    table_name="customer_trusted"
)

accelerometer_df = glueContext.create_dynamic_frame.from_catalog(
    database="your_glue_db",
    table_name="accelerometer_trusted"
)

curated_df = Join.apply(
    frame1=customer_df,
    frame2=accelerometer_df,
    keys1=["email"],
    keys2=["user"]
).select_fields(["serialNumber", "customerName", "email"])

curated_df = curated_df.drop_duplicates()

glueContext.write_dynamic_frame.from_options(
    frame=curated_df,
    connection_type="s3",
    connection_options={"path": "s3://your-bucket/curated/customer_curated/"},
    format="json"
)

job.commit()
