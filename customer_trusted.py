import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_null_rows
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1702898838528 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://lake-house-stedi/customer_landing/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1702898838528",
)

# Script generated for node Remove Null Rows
RemoveNullRows_node1702900843084 = AmazonS3_node1702898838528.gs_null_rows(
    extended=True
)

# Script generated for node Filter
Filter_node1702900946384 = Filter.apply(
    frame=RemoveNullRows_node1702900843084,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filter_node1702900946384",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1702899055771 = glueContext.write_dynamic_frame.from_options(
    frame=Filter_node1702900946384,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://lake-house-stedi/customer_landing/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrusted_node1702899055771",
)

job.commit()
