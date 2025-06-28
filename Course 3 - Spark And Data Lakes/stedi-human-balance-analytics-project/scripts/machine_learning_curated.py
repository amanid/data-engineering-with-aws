"""
This Glue job creates a final dataset by joining step trainer data
with accelerometer data based on timestamp alignment.
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

step_trainer_df = glueContext.create_dynamic_frame.from_catalog(
    database="your_glue_db",
    table_name="step_trainer_trusted"
)

accelerometer_df = glueContext.create_dynamic_frame.from_catalog(
    database="your_glue_db",
    table_name="accelerometer_trusted"
)

ml_curated_df = Join.apply(
    frame1=step_trainer_df,
    frame2=accelerometer_df,
    keys1=["sensorReadingTime"],
    keys2=["timeStamp"]
)

glueContext.write_dynamic_frame.from_options(
    frame=ml_curated_df,
    connection_type="s3",
    connection_options={"path": "s3://your-bucket/curated/machine_learning/"},
    format="json"
)

job.commit()
