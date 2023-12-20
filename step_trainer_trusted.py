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

# Script generated for node step_trainer_landing
step_trainer_landing_node1703065168374 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="step_trainer_landing_node1703065168374",
)

# Script generated for node customer_curated
customer_curated_node1703065176810 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="customer_curated_node1703065176810",
)

# Script generated for node Join
Join_node1703065265840 = Join.apply(
    frame1=step_trainer_landing_node1703065168374,
    frame2=customer_curated_node1703065176810,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1703065265840",
)

# Script generated for node Drop Fields
DropFields_node1703065352781 = DropFields.apply(
    frame=Join_node1703065265840,
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
    transformation_ctx="DropFields_node1703065352781",
)

# Script generated for node step_landing_trusted
step_landing_trusted_node1703065445898 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1703065352781,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://lake-house-stedi/step_trainer_landing/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="step_landing_trusted_node1703065445898",
)

job.commit()
