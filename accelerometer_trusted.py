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

# Script generated for node accelerometer_landing
accelerometer_landing_node1703061474056 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_node1703061474056",
)

# Script generated for node customer_trusted
customer_trusted_node1703061620875 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1703061620875",
)

# Script generated for node customer privacy join
customerprivacyjoin_node1703061569715 = Join.apply(
    frame1=accelerometer_landing_node1703061474056,
    frame2=customer_trusted_node1703061620875,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="customerprivacyjoin_node1703061569715",
)

# Script generated for node Drop Fields
DropFields_node1703061893643 = DropFields.apply(
    frame=customerprivacyjoin_node1703061569715,
    paths=[
        "serialNumber",
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "email",
        "phone",
        "lastUpdateDate",
    ],
    transformation_ctx="DropFields_node1703061893643",
)

# Script generated for node Amazon S3
AmazonS3_node1703062189919 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1703061893643,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://lake-house-stedi/accelerometer_landing/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1703062189919",
)

job.commit()
