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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1699896529338 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-0911/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node1699896529338",
)

# Script generated for node Customer Privacy
CustomerPrivacy_node1699549947832 = Join.apply(
    frame1=customer_trusted_node1699537399209,
    frame2=accelerometer_trusted_node1699896529338,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="CustomerPrivacy_node1699549947832",
)

# Script generated for node Drop field
SqlQuery469 = """
select distinct serialnumber, 
  sharewithpublicasofdate , 
  birthday , 
  registrationdate , 
  sharewithresearchasofdate , 
  customername , 
  sharewithfriendsasofdate , 
  email , 
  lastupdatedate , 
  phone
from myDataSource

"""
Dropfield_node1699897696141 = sparkSqlQuery(
    glueContext,
    query=SqlQuery469,
    mapping={"myDataSource": CustomerPrivacy_node1699549947832},
    transformation_ctx="Dropfield_node1699897696141",
)

# Script generated for node Customer Curated
CustomerCurated_node1699537481412 = glueContext.getSink(
    path="s3://stedi-lake-house-0911/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node1699537481412",
)
CustomerCurated_node1699537481412.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
CustomerCurated_node1699537481412.setFormat("json")
CustomerCurated_node1699537481412.writeFrame(Dropfield_node1699897696141)
job.commit()
