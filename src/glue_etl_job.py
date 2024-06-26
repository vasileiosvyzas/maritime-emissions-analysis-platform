import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame

# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, regexp_extract, when
    import pyspark.sql.functions as F

    df = dfc.select(list(dfc.keys())[0]).toDF()

    extracted_eiv_type = when(
            col("technical_efficiency").isNotNull(),
            when(
                col("technical_efficiency") != "Not Applicable",
                when(col("technical_efficiency").contains("EIV"), "EIV")
                .when(col("technical_efficiency").contains("EEDI"), "EEDI")
                .otherwise(None),
            ),
    ).otherwise(None)

    df = df.withColumn("technical_efficiency_type", extracted_eiv_type)

    extracted_eiv_value = when(
            col("technical_efficiency").isNotNull(),
            when(
                col("technical_efficiency") != "Not Applicable",
                regexp_extract(col("technical_efficiency"), r"\d+(\.\d+)?", 0),
            ),
    ).otherwise(None)

    df = df.withColumn("technical_efficiency_value", extracted_eiv_value)
    df = df.withColumn("technical_efficiency_unit", F.lit("gCO₂/t·nm"))
    DyF = DynamicFrame.fromDF(df, glueContext, "tech_efficiency_processing")
    return DynamicFrameCollection({"CustomTransform0": DyF}, glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1719433405033 = glueContext.create_dynamic_frame.from_catalog(database="ship-emissions-database", table_name="interim_data", transformation_ctx="AWSGlueDataCatalog_node1719433405033")

# Script generated for node Custom Transform
CustomTransform_node1719433433899 = MyTransform(glueContext, DynamicFrameCollection({"AWSGlueDataCatalog_node1719433405033": AWSGlueDataCatalog_node1719433405033}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1719433683684 = SelectFromCollection.apply(dfc=CustomTransform_node1719433433899, key=list(CustomTransform_node1719433433899.keys())[0], transformation_ctx="SelectFromCollection_node1719433683684")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1719433468617 = glueContext.write_dynamic_frame.from_catalog(frame=SelectFromCollection_node1719433683684, database="ship-emissions-database", table_name="clean_emissions_data", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node1719433468617")

job.commit()