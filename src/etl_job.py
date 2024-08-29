import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import trim, col, when, concat_ws, isnull, length, sum, regexp_extract
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
glue_df = glueContext.create_dynamic_frame.from_catalog(
    database="ship-emissions-database", 
    table_name="crawler_interim",
    transformation_ctx = "datasource0"
)
glue_df.printSchema()
# Convert DynamicFrame to DataFrame
spark_df = glue_df.toDF()
spark_df = spark_df.drop(*('d.1', 'additional information to facilitate the understanding of the reported average operational energy efficiency indicators')) 
print(f"Number of rows: {spark_df.count()} and number of columns: {len(spark_df.columns)}")
spark_df.select('a', 'b', 'c', 'd').show()
# List of columns to be processed
columns = ["a", "b", "c", "d"]

# Trim whitespace for all specified columns
for column in columns:
    spark_df = spark_df.withColumn(column, trim(col(column)))

# Replace empty strings and fill nulls with "Unknown" in the specified columns
spark_df = spark_df.na.replace("", "Unknown", subset=columns).fillna("Unknown", subset=columns)
spark_df.select('a', 'b', 'c', 'd').show()
# Create the monitoring_methods column
spark_df = spark_df.withColumn(
    "monitoring_methods",
    concat_ws(
        ", ",
        when(col("a") == "Yes", "a"),
        when(col("b") == "Yes", "b"),
        when(col("c") == "Yes", "c"),
        when(col("d") == "Yes", "d")
    )
)

spark_df = spark_df.fillna("Unknown", subset=["monitoring_methods"])
spark_df.select('a', 'b', 'c', 'd', 'monitoring_methods').show()
spark_df.groupBy('monitoring_methods').count().show()
spark_df = spark_df.withColumn(column, trim(col("monitoring_methods")))

# Replace empty strings and fill nulls with "missing" in the specified columns
spark_df = spark_df.na.replace("", "missing", subset=["monitoring_methods"]).fillna("missing", subset=["monitoring_methods"])
spark_df.groupBy('monitoring_methods').count().show()
spark_df = spark_df.drop(*['a', 'b', 'c', 'd'])
spark_df.printSchema()
def convert_columns_to_double(df, columns):
    for column in columns:
        # Check if the column type is string
        if dict(df.dtypes)[column] == 'string':
            df = df.withColumn(
                column,
                F.when(F.trim(F.col(column)) == "Division by zero!", F.lit(0.0))
                .when(F.trim(F.col(column)).isin("", "null", "NULL", "Null"), None)
                .otherwise(F.col(column).cast(DoubleType()))
            )
            
    # Fill null values with 0.0
    df = df.fillna(0.0, subset=columns)
    return df

# List of columns to process
columns = ['total fuel consumption [m tonnes]', 'total co₂ emissions [m tonnes]', 'co₂ emissions from all voyages between ports under a ms jurisdiction [m tonnes]', 'co₂ emissions from all voyages which departed from ports under a ms jurisdiction [m tonnes]', 'co₂ emissions from all voyages to ports under a ms jurisdiction [m tonnes]', 'co₂ emissions which occurred within ports under a ms jurisdiction at berth [m tonnes]', 'annual total time spent at sea [hours]', 'total time spent at sea [hours]', 'co₂ emissions assigned to passenger transport [m tonnes]', 'co₂ emissions assigned to freight transport [m tonnes]', 'fuel consumptions assigned to on laden [m tonnes]', 'co₂ emissions assigned to on laden [m tonnes]', 'through ice [n miles]', 'total time spent at sea through ice [hours]', 'fuel consumption per transport work (pax) on laden voyages [g / pax · n miles]', 'co₂ emissions per transport work (pax) on laden voyages [g co₂ / pax · n miles]']


# Apply the conversion
spark_df = convert_columns_to_double(spark_df, columns)
# Total number of rows in the DataFrame
total_rows = spark_df.count()

# Calculate the percentage of missing values for each column
missing_percentage = spark_df.select([
    (
        sum(
            when(col(c).isNull(), 1).otherwise(0)
        ) / total_rows * 100
    ).alias(c)
    for c in spark_df.columns
])

# Show the result
missing_percentage.show()
columns = ['co₂ emissions assigned to passenger transport [m tonnes]', 
           'co₂ emissions assigned to freight transport [m tonnes]', 
           'fuel consumptions assigned to on laden [m tonnes]', 
           'co₂ emissions assigned to on laden [m tonnes]', 
           'fuel consumption per transport work (pax) on laden voyages [g / pax · n miles]', 
           'co₂ emissions per transport work (pax) on laden voyages [g co₂ / pax · n miles]', 
           'through ice [n miles]', 
           'total time spent at sea through ice [hours]']

spark_df = spark_df.fillna(0.0, subset=columns)
# Total number of rows in the DataFrame
total_rows = spark_df.count()

# Calculate the percentage of missing values for each column
missing_percentage = spark_df.select([
    (
        sum(
            when(col(c).isNull(), 1).otherwise(0)
        ) / total_rows * 100
    ).alias(c)
    for c in spark_df.columns
])

# Show the result
missing_percentage.show()
spark_df.select('technical efficiency').show()
spark_df.printSchema()
spark_df.printSchema()
extracted_eiv_type = when(
    col("technical efficiency").isNotNull(),
    when(
        col("technical efficiency") != "Not Applicable", when(col("technical efficiency").contains("EIV"), "EIV").when(col("technical efficiency").contains("EEDI"), "EEDI").otherwise(None),
    )
).otherwise(None)

spark_df = spark_df.withColumn("technical_efficiency_type", extracted_eiv_type)

extracted_eiv_value = when(
    col("technical efficiency").isNotNull(),
    when(
        col("technical efficiency") != "Not Applicable",regexp_extract(col("technical efficiency"), r"\d+(\.\d+)?", 0))
).otherwise(None)

spark_df = spark_df.withColumn("technical_efficiency_value", extracted_eiv_value)
spark_df = spark_df.withColumn("technical_efficiency_unit", F.lit("gCO₂/t·nm"))
spark_df.select('technical efficiency', 'technical_efficiency_type', 'technical_efficiency_value', 'technical_efficiency_unit').show()
spark_df = spark_df.drop('technical efficiency')
spark_df.printSchema()
# Total number of rows in the DataFrame
total_rows = spark_df.count()

# Calculate the percentage of missing values for each column
missing_percentage = spark_df.select([
    (
        sum(
            when(col(c).isNull(), 1).otherwise(0)
        ) / total_rows * 100
    ).alias(c)
    for c in spark_df.columns
])

# Show the result
missing_percentage.show()
# Replace empty strings and fill nulls with "missing" in the specified columns
spark_df = spark_df.na.replace("", "missing", subset=["technical_efficiency_value", "technical_efficiency_type"]).fillna("missing", subset=["technical_efficiency_value", "technical_efficiency_type"])
# Total number of rows in the DataFrame
total_rows = spark_df.count()

# Calculate the percentage of missing values for each column
missing_percentage = spark_df.select([
    (
        sum(
            when(col(c).isNull(), 1).otherwise(0)
        ) / total_rows * 100
    ).alias(c)
    for c in spark_df.columns
])

# Show the result
missing_percentage.show()
DyF = DynamicFrame.fromDF(spark_df, glueContext, "etl_convert")
s3output = glueContext.getSink(
  path="s3://my_bucket/clean/",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output_final_step",
)
s3output.setCatalogInfo(
  catalogDatabase="ship-emissions-database", catalogTableName="clean_emissions"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(DyF)

job.commit()