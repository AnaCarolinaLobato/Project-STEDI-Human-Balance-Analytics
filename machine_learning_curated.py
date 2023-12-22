import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step Trusted Landing Zone
StepTrusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrusted",
)

# Script generated for node Customer Accelerometer Trusted
AccelerometerTrusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted",
)

# Script generated for node Step Trusted and Accelerometer Trusted Join
SqlQuery1 = """
SELECT
    step_trainer_trusted.sensorReadingTime,
    step_trainer_trusted.serialNumber,
    step_trainer_trusted.distanceFromObject,
    accelerometer_trusted.timeStamp,
    accelerometer_trusted.x,
    accelerometer_trusted.y,
    accelerometer_trusted.z,
    accelerometer_trusted.user
FROM
    step_trainer_trusted,
    accelerometer_trusted
WHERE
    step_trainer_trusted.sensorReadingTime = accelerometer_trusted.timeStamp
"""

StepTrustedandAccelerometerTrustedJoin = sparkSqlQuery(
    glueContext,
    query=SqlQuery1,
    mapping={
        "step_trainer_trusted": StepTrusted,
        "accelerometer_trusted": AccelerometerTrusted,
    },
    transformation_ctx="StepTrustedandAccelerometerTrustedJoin",
)

# Script generated for node SQL Distinct Query
SqlQuery0 = """
SELECT DISTINCT * FROM myDataSource
"""

SQLDistinctQuery1 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": StepTrustedandAccelerometerTrustedJoin},
    transformation_ctx="SQLDistinctQuery1",
)

# Script generated for node machine learning curated Zone
machinelearningcuratedZone = glueContext.getSink(
    path="s3://lake-house-stedi/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="machinelearningcuratedZone",
)

machinelearningcuratedZone.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
machinelearningcuratedZone.setFormat("json")
machinelearningcuratedZone.writeFrame(SQLDistinctQuery1)

job.commit()
