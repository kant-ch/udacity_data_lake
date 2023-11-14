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

# Script generated for node customer_curated
customer_curated_node1699897000988 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="customer_curated_node1699897000988",
)

# Script generated for node step_trainer_landing
step_trainer_landing_node1699897017005 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-0911/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_node1699897017005",
)

# Script generated for node Join
SqlQuery478 = """
select a.* 
from a
inner join b on a.serialnumber = b.serialnumber
"""
Join_node1699899432026 = sparkSqlQuery(
    glueContext,
    query=SqlQuery478,
    mapping={
        "a": step_trainer_landing_node1699897017005,
        "b": customer_curated_node1699897000988,
    },
    transformation_ctx="Join_node1699899432026",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1699897229869 = glueContext.getSink(
    path="s3://stedi-lake-house-0911/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_node1699897229869",
)
step_trainer_trusted_node1699897229869.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
step_trainer_trusted_node1699897229869.setFormat("json")
step_trainer_trusted_node1699897229869.writeFrame(Join_node1699899432026)
job.commit()
