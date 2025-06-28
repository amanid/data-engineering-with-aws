"""
This Glue job reads step trainer landing data and filters it
based on customers curated to share data.
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

step_trainer_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://your-bucket/landing/step_trainer_landing/"]},
    format="json"
)

customer_curated_df = glueContext.create_dynamic_frame.from_catalog(
    database="your_glue_db",
    table_name="customer_curated"
)

trusted_step_df = Join.apply(
    frame1=step_trainer_df,
    frame2=customer_curated_df,
    keys1=["serialNumber"],
    keys2=["serialNumber"]
)

glueContext.write_dynamic_frame.from_options(
    frame=trusted_step_df,
    connection_type="s3",
    connection_options={"path": "s3://your-bucket/trusted/step_trainer_trusted/"},
    format="json"
)

job.commit()
