"""
This Glue job creates a final dataset by joining trusted step trainer data
with trusted accelerometer data based on timestamp alignment, producing
a curated dataset for machine learning.
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import Join

# Initialize Glue job context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load trusted Step Trainer data from Glue Catalog
step_trainer_df = glueContext.create_dynamic_frame.from_catalog(
    database="my_glue_db",
    table_name="step_trainer_trusted"
)

# Load trusted Accelerometer data from Glue Catalog
accelerometer_df = glueContext.create_dynamic_frame.from_catalog(
    database="my_glue_db",
    table_name="accelerometer_trusted"
)

# Perform join based on timestamp alignment
ml_curated_df = Join.apply(
    frame1=step_trainer_df,
    frame2=accelerometer_df,
    keys1=["sensorReadingTime"],
    keys2=["timeStamp"]
)

# Write the curated dataset to S3 for ML training
glueContext.write_dynamic_frame.from_options(
    frame=ml_curated_df,
    connection_type="s3",
    connection_options={"path": "s3://nd-parent-bucket/curated/machine_learning/"},
    format="json"
)

job.commit()
