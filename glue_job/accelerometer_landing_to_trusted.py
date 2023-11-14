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

# Script generated for node customer_trusted
customer_trusted_node1699537399209 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-0911/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1699537399209",
)

# Script generated for node accelerometer_landing
accelerometer_landing_node1699549746142 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-0911/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1699549746142",
)

# Script generated for node Customer Privacy
CustomerPrivacy_node1699549947832 = Join.apply(
    frame1=customer_trusted_node1699537399209,
    frame2=accelerometer_landing_node1699549746142,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="CustomerPrivacy_node1699549947832",
)

# Script generated for node Drop Fields
DropFields_node1699550071408 = DropFields.apply(
    frame=CustomerPrivacy_node1699549947832,
    paths=[
        "phone",
        "email",
        "lastUpdateDate",
        "shareWithFriendsAsOfDate",
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
    ],
    transformation_ctx="DropFields_node1699550071408",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1699537481412 = glueContext.getSink(
    path="s3://stedi-lake-house-0911/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1699537481412",
)
AccelerometerTrusted_node1699537481412.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1699537481412.setFormat("json")
AccelerometerTrusted_node1699537481412.writeFrame(DropFields_node1699550071408)
job.commit()
