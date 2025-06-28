"""
This Glue job loads the raw customer data from the landing zone,
filters out customers who did not consent to share data for research,
and saves the filtered results to the trusted zone.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

customer_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://your-bucket/landing/customer_landing/"]},
    format="json"
)

filtered_df = Filter.apply(frame=customer_df, f=lambda x: x["shareWithResearchAsOfDate"] is not None)

glueContext.write_dynamic_frame.from_options(
    frame=filtered_df,
    connection_type="s3",
    connection_options={"path": "s3://your-bucket/trusted/customer_trusted/"},
    format="json"
)

job.commit()
