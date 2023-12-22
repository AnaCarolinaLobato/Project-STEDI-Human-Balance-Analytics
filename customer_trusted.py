import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Get job arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Initialize GlueContext, SparkContext, and Job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
s3_source_frame = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://lake-house-stedi/customer_landing/"],
        "recurse": True,
    },
    transformation_ctx="s3_source_frame",
)

# Script generated for node Filter
filtered_frame = Filter.apply(
    frame=s3_source_frame,
    f=lambda row: (row["shareWithResearchAsOfDate"] is not None),
    transformation_ctx="filtered_frame",
)

# Script generated for node Customer Trusted
glueContext.write_dynamic_frame.from_options(
    frame=filtered_frame,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://lake-house-stedi/customer_landing/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="customer_trusted_frame",
)

# Commit the job
job.commit()

