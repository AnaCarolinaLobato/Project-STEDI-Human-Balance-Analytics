import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer Landing
accelerometerLanding_node1702885895743 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://lake-house-stedi/accelerometer_landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometerLanding_node1702885895743",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1702885393040 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing",
    transformation_ctx="CustomerTrusted_node1702885393040",
)

# Script generated for node Privacy Join
PrivacyJoin_node1702886701554 = Join.apply(
    frame1=CustomerTrusted_node1702885393040,
    frame2=accelerometerLanding_node1702885895743,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="PrivacyJoin_node1702886701554",
)

# Script generated for node Drop Fields
DropFields_node1702886774518 = DropFields.apply(
    frame=PrivacyJoin_node1702886701554,
    paths=["email", "phone"],
    transformation_ctx="DropFields_node1702886774518",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1702886906761 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1702886774518,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://lake-house-stedi/accelerometer_landing/",
        "compression": "snappy",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node1702886906761",
)

job.commit()
