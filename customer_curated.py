import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer_trusted
customer_trusted_node1703212229238 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://lake-house-stedi/customer_landing/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1703212229238",
)

# Script generated for node acceleromenter_trusted
acceleromenter_trusted_node1703212227831 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="accelerometer_trusted",
        transformation_ctx="acceleromenter_trusted_node1703212227831",
    )
)

# Script generated for node Join
Join_node1703212678990 = Join.apply(
    frame1=acceleromenter_trusted_node1703212227831,
    frame2=customer_trusted_node1703212229238,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1703212678990",
)

# Script generated for node Filter
Filter_node1703212905731 = Filter.apply(
    frame=Join_node1703212678990,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filter_node1703212905731",
)

# Script generated for node Drop Fields
DropFields_node1703212984680 = DropFields.apply(
    frame=Filter_node1703212905731,
    paths=["z", "user", "y", "x", "timestamp"],
    transformation_ctx="DropFields_node1703212984680",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1703217771523 = DynamicFrame.fromDF(
    DropFields_node1703212984680.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1703217771523",
)

# Script generated for node Amazon S3
AmazonS3_node1703214061895 = glueContext.getSink(
    path="s3://lake-house-stedi/customer_landing/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1703214061895",
)
AmazonS3_node1703214061895.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
AmazonS3_node1703214061895.setFormat("json")
AmazonS3_node1703214061895.writeFrame(DropDuplicates_node1703217771523)
job.commit()
