import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1702547827728 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://lake-house-stedi/Customer_landing/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1702547827728",
)

# Script generated for node PrivacyFilter
PrivacyFilter_node1702547876727 = Filter.apply(
    frame=AmazonS3_node1702547827728,
    f=lambda row: (not (row["sharewithresearchasofdate"] == 0)),
    transformation_ctx="PrivacyFilter_node1702547876727",
)

# Script generated for node Trusted Customer Zone
TrustedCustomerZone_node1702548402357 = glueContext.write_dynamic_frame.from_options(
    frame=PrivacyFilter_node1702547876727,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://lake-house-stedi/customer_landing/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="TrustedCustomerZone_node1702548402357",
)

job.commit()
