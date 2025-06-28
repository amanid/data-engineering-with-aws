"""
This Glue job reads Step Trainer IoT data from the landing zone
and filters it to include only records that match customers who
consented to share data (customer_curated). The result is stored
in the trusted zone.
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import Join

# Initialize the Glue job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load Step Trainer raw data from landing zone
step_trainer_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://nd-parent-bucket/landing/step_trainer_landing/"]},
    format="json"
)

# Load curated customer list from Glue Catalog
customer_curated_df = glueContext.create_dynamic_frame.from_catalog(
    database="my_glue_db",
    table_name="customer_curated"
)

# Join on serialNumber to retain only records from consenting customers
trusted_step_df = Join.apply(
    frame1=step_trainer_df,
    frame2=customer_curated_df,
    keys1=["serialNumber"],
    keys2=["serialNumber"]
)

# Write output to trusted zone in S3
glueContext.write_dynamic_frame.from_options(
    frame=trusted_step_df,
    connection_type="s3",
    connection_options={"path": "s3://nd-parent-bucket/trusted/step_trainer_trusted/"},
    format="json"
)

job.commit()
