"""
This Glue job loads the raw customer data from the landing zone,
filters out customers who did not consent to share data for research,
and saves the filtered results to the trusted zone.
"""

import sys
from awsglue.transforms import Filter
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue job context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load raw customer data from the landing zone
customer_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://nd-parent-bucket/landing/customer_landing/"]},
    format="json"
)

# Filter for only customers who consented to research
filtered_df = Filter.apply(
    frame=customer_df,
    f=lambda record: record["shareWithResearchAsOfDate"] is not None
)

# Write trusted customer data to S3
glueContext.write_dynamic_frame.from_options(
    frame=filtered_df,
    connection_type="s3",
    connection_options={"path": "s3://nd-parent-bucket/trusted/customer_trusted/"},
    format="json"
)

job.commit()
