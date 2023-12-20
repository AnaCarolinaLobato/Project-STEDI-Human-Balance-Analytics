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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1703072338658 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1703072338658",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1703072342877 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_node1703072342877",
)

# Script generated for node Join
Join_node1703072427177 = Join.apply(
    frame1=step_trainer_trusted_node1703072342877,
    frame2=accelerometer_trusted_node1703072338658,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="Join_node1703072427177",
)

# Script generated for node Drop Fields
DropFields_node1703072455245 = DropFields.apply(
    frame=Join_node1703072427177,
    paths=["user"],
    transformation_ctx="DropFields_node1703072455245",
)

# Script generated for node machine_learning_curated
machine_learning_curated_node1703072480520 = (
    glueContext.write_dynamic_frame.from_options(
        frame=DropFields_node1703072455245,
        connection_type="s3",
        format="glueparquet",
        connection_options={
            "path": "s3://lake-house-stedi/step_trainer_landing/curated/",
            "partitionKeys": [],
        },
        format_options={"compression": "snappy"},
        transformation_ctx="machine_learning_curated_node1703072480520",
    )
)

job.commit()
