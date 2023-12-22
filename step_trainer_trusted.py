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

# Script generated for node Step Trainer Landing Zone
StepTrainerLandingZone = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="step_trainer_landing",
        transformation_ctx="StepTrainerLandingZone",
    )
)
# Script generated for node Customer Curated Zone
CustomerCuratedZone = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCuratedZone",
)
# Script generated for node Step Trainer and Customer Curated Join
SqlQuery1 = """
select step_trainer_landing.sensorReadingTime, step_trainer_landing.serialNumber, step_trainer_landing.distanceFromObject
from step_trainer_landing, customer_curated
where step_trainer_landing.serialNumber = customer_curated.serialNumber
"""
StepTrainerandCustomerCuratedJoin = sparkSqlQuery(
    glueContext,
    query=SqlQuery1,
    mapping={
        "step_trainer_landing": StepTrainerLandingZone,
        "customer_curated": CustomerCuratedZone,
    },
    transformation_ctx="StepTrainerandCustomerCuratedJoin",
)

# Script generated for node SQL Distinct Query
SqlQuery0 = """
select distinct * from myDataSource
"""
SQLDistinctQuery = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": StepTrainerandCustomerCuratedJoin},
    transformation_ctx="SQLDistinctQuery",
)

# Script generated for node Step Trainer Trusted Zone
StepTrainerTrustedZone = glueContext.getSink(
    path="s3://lake-house-stedi/step_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrustedZone",
)
StepTrainerTrustedZone.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
StepTrainerTrustedZone.setFormat("json")
StepTrainerTrustedZone.writeFrame(SQLDistinctQuery)
job.commit()
