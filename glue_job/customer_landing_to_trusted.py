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
AmazonS3_node1699537399209 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-0911/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1699537399209",
)

# Script generated for node Filter
Filter_node1699537414455 = Filter.apply(
    frame=AmazonS3_node1699537399209,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filter_node1699537414455",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1699537481412 = glueContext.getSink(
    path="s3://stedi-lake-house-0911/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1699537481412",
)
CustomerTrusted_node1699537481412.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
CustomerTrusted_node1699537481412.setFormat("json")
CustomerTrusted_node1699537481412.writeFrame(Filter_node1699537414455)
job.commit()
