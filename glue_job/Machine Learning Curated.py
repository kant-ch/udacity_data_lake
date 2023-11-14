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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1699902896229 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_node1699902896229",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1699902916913 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1699902916913",
)

# Script generated for node Filter Data
SqlQuery571 = """
select acc.*, step.distancefromobject 
from step
inner join acc on step.sensorreadingtime = acc.timestamp
"""
FilterData_node1699902959413 = sparkSqlQuery(
    glueContext,
    query=SqlQuery571,
    mapping={
        "step": step_trainer_trusted_node1699902896229,
        "acc": accelerometer_trusted_node1699902916913,
    },
    transformation_ctx="FilterData_node1699902959413",
)

# Script generated for node machine_learning_curated
machine_learning_curated_node1699903051608 = glueContext.getSink(
    path="s3://stedi-lake-house-0911/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_curated_node1699903051608",
)
machine_learning_curated_node1699903051608.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
machine_learning_curated_node1699903051608.setFormat("json")
machine_learning_curated_node1699903051608.writeFrame(FilterData_node1699902959413)
job.commit()
