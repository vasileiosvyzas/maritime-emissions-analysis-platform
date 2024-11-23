
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
    table_name="interim",
    transformation_ctx = "datasource0"
)
glue_df.printSchema()
spark_df = glue_df.toDF()
spark_df = spark_df.drop(*('d.1', 'additional information to facilitate the understanding of the reported average operational energy efficiency indicators')) 
print(f"Number of rows: {spark_df.count()} and number of columns: {len(spark_df.columns)}")
spark_df.groupby('year').count().show()
# List of columns to be processed
columns = ["a", "b", "c", "d"]

# Trim whitespace for all specified columns
for column in columns:
    spark_df = spark_df.withColumn(column, trim(col(column)))

# Replace empty strings and fill nulls with "Unknown" in the specified columns
spark_df = spark_df.na.replace("", "Unknown", subset=columns).fillna("Unknown", subset=columns)

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

spark_df = spark_df.withColumn(column, trim(col("monitoring_methods")))

# Replace empty strings and fill nulls with "missing" in the specified columns
spark_df = spark_df.na.replace("", "missing", subset=["monitoring_methods"]).fillna("missing", subset=["monitoring_methods"])

spark_df = spark_df.drop(*['a', 'b', 'c', 'd'])
spark_df.groupBy('monitoring_methods').count().show()
def convert_columns_to_double(df, columns):
    for column in columns:
        # Check if the column type is string
        if dict(df.dtypes)[column] == 'string':
            df = df.withColumn(
                column,
                F.when(F.trim(F.col(column)) == "Division by zero!", F.lit(0.0))
                .when(F.trim(F.col(column)).isin("", "null", "NULL", "Null", "Missing"), None)
                .otherwise(F.col(column).cast(DoubleType()))
            )
            
    # Fill null values with 0.0
    df = df.fillna(0.0, subset=columns)
    return df
# List of columns to process
columns = [
    "total fuel consumption [m tonnes]",
    "total co₂ emissions [m tonnes]",
    "co₂ emissions from all voyages between ports under a ms jurisdiction [m tonnes]",
    "co₂ emissions from all voyages which departed from ports under a ms jurisdiction [m tonnes]",
    "co₂ emissions from all voyages to ports under a ms jurisdiction [m tonnes]",
    "co₂ emissions which occurred within ports under a ms jurisdiction at berth [m tonnes]",
    "annual time spent at sea [hours]",
    "time spent at sea [hours]",
    "co₂ emissions assigned to passenger transport [m tonnes]",
    "co₂ emissions assigned to freight transport [m tonnes]",
    "fuel consumptions assigned to on laden [m tonnes]",
    "co₂ emissions assigned to on laden [m tonnes]",
    "through ice [n miles]",
    "total time spent at sea through ice [hours]",
    "fuel consumption per transport work (pax) on laden voyages [g / pax · n miles]",
    "co₂ emissions per transport work (pax) on laden voyages [g co₂ / pax · n miles]",
    "co₂ emissions assigned to passenger transport [m tonnes]",
    "co₂ emissions assigned to freight transport [m tonnes]",
    "fuel consumptions assigned to on laden [m tonnes]",
    "co₂ emissions assigned to on laden [m tonnes]",
    "fuel consumption per transport work (pax) on laden voyages [g / pax · n miles]",
    "co₂ emissions per transport work (pax) on laden voyages [g co₂ / pax · n miles]",
    "through ice [n miles]",
    "total time spent at sea through ice [hours]",
    "annual average co₂ emissions per distance [kg co₂ / n mile]",
    "annual average co₂ emissions per transport work (dwt) [g co₂ / dwt carried · n miles]",
    "annual average co₂ emissions per transport work (freight) [g co₂ / m tonnes · n miles]",
    "annual average co₂ emissions per transport work (mass) [g co₂ / m tonnes · n miles]",
    "annual average co₂ emissions per transport work (pax) [g co₂ / pax · n miles]",
    "annual average co₂ emissions per transport work (volume) [g co₂ / m³ · n miles]",
    "annual average fuel consumption per distance [kg / n mile]",
    "annual average fuel consumption per transport work (dwt) [g / dwt carried · n miles]",
    "annual average fuel consumption per transport work (freight) [g / m tonnes · n miles]",
    "annual average fuel consumption per transport work (mass) [g / m tonnes · n miles]",
    "annual average fuel consumption per transport work (pax) [g / pax · n miles]",
    "annual average fuel consumption per transport work (volume) [g / m³ · n miles]",
    "average density of the cargo transported [m tonnes / m³]",
    "co₂ emissions per distance on laden voyages [kg co₂ / n mile]",
    "co₂ emissions per transport work (dwt) on laden voyages [g co₂ / dwt carried · n miles]",
    "co₂ emissions per transport work (freight) on laden voyages [g co₂ / m tonnes · n miles]",
    "co₂ emissions per transport work (mass) on laden voyages [g co₂ / m tonnes · n miles]",
    "co₂ emissions per transport work (volume) on laden voyages [g co₂ / m³ · n miles]",
    "fuel consumption per distance on laden voyages [kg / n mile]",
    "fuel consumption per transport work (dwt) on laden voyages [g / dwt carried · n miles]",
    "fuel consumption per transport work (freight) on laden voyages [g / m tonnes · n miles]",
    "fuel consumption per transport work (mass) on laden voyages [g / m tonnes · n miles]",
    "fuel consumption per transport work (volume) on laden voyages [g / m³ · n miles]",
]

# Apply the conversion
spark_df = convert_columns_to_double(spark_df, columns)
spark_df.printSchema()
# columns = ['co₂ emissions assigned to passenger transport [m tonnes]', 
#            'co₂ emissions assigned to freight transport [m tonnes]', 
#            'fuel consumptions assigned to on laden [m tonnes]', 
#            'co₂ emissions assigned to on laden [m tonnes]', 
#            'fuel consumption per transport work (pax) on laden voyages [g / pax · n miles]', 
#            'co₂ emissions per transport work (pax) on laden voyages [g co₂ / pax · n miles]', 
#            'through ice [n miles]', 
#            'total time spent at sea through ice [hours]']

# spark_df = spark_df.fillna(0.0, subset=columns)
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
extracted_eiv_type = when(
    col("technical efficiency").isNotNull(),
    when(
        col("technical efficiency") != "Not Applicable",
        when(col("technical efficiency").contains("EIV"), "EIV")
        .when(col("technical efficiency").contains("EEDI"), "EEDI")
        .when(col("technical efficiency").contains("EEXI"), "EEXI")
        .when(col("technical efficiency").contains("Not Applicable"), "Not Applicable")
        .otherwise(None),
    ),
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
spark_df.groupBy('technical_efficiency_type').count().show()
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
spark_df.printSchema()
print(f"Number of rows: {spark_df.count()} and number of columns: {len(spark_df.columns)}")
column_mapping = {
    "imo number": "imo_number",
    "name": "name",
    "ship type": "ship_type",
    "reporting period": "reporting_period",
    "port of registry": "port_of_registry",
    "home port": "home_port",
    "ice class": "ice_class",
    "doc issue date": "doc_issue_date",
    "doc expiry date": "doc_expiry_date",
    "verifier number": "verifier_number",
    "verifier name": "verifier_name",
    "verifier nab": "verifier_nab",
    "verifier city": "verifier_city",
    "verifier accreditation number": "verifier_accreditation_number",
    "verifier country": "verifier_country",
    "total fuel consumption [m tonnes]": "total_fuel_consumption",
    "fuel consumptions assigned to on laden [m tonnes]": "fuel_consumptions_assigned_to_on_laden",
    "total co₂ emissions [m tonnes]": "total_co2_emissions",
    "co₂ emissions from all voyages between ports under a ms jurisdiction [m tonnes]": "co2_emissions_from_all_voyages_between_ports_under_a_ms_jurisdiction",
    "co₂ emissions from all voyages which departed from ports under a ms jurisdiction [m tonnes]": "co2_emissions_from_all_voyages_which_departed_from_ports_under_a_ms_jurisdiction",
    "co₂ emissions from all voyages to ports under a ms jurisdiction [m tonnes]": "co2_emissions_from_all_voyages_to_ports_under_a_ms_jurisdiction",
    "co₂ emissions which occurred within ports under a ms jurisdiction at berth [m tonnes]": "co2_emissions_which_occurred_within_ports_under_a_ms_jurisdiction_at_berth",
    "co₂ emissions assigned to passenger transport [m tonnes]": "co2_emissions_assigned_to_passenger_transport",
    "co₂ emissions assigned to freight transport [m tonnes]": "co2_emissions_assigned_to_freight_transport",
    "co₂ emissions assigned to on laden [m tonnes]": "co2_emissions_assigned_to_on_laden",
    "annual time spent at sea [hours]": "annual_time_spent_at_sea",
    "annual average fuel consumption per distance [kg / n mile]": "annual_average_fuel_consumption_per_distance",
    "annual average fuel consumption per transport work (mass) [g / m tonnes · n miles]": "annual_average_fuel_consumption_per_transport_work_mass",
    "annual average fuel consumption per transport work (volume) [g / m³ · n miles]": "annual_average_fuel_consumption_per_transport_work_volume",
    "annual average fuel consumption per transport work (dwt) [g / dwt carried · n miles]": "annual_average_fuel_consumption_per_transport_work_dwt",
    "annual average fuel consumption per transport work (pax) [g / pax · n miles]": "annual_average_fuel_consumption_per_transport_work_pax",
    "annual average fuel consumption per transport work (freight) [g / m tonnes · n miles]": "annual_average_fuel_consumption_per_transport_work_freight",
    "annual average co₂ emissions per distance [kg co₂ / n mile]": "annual_average_co2_emissions_per_distance",
    "annual average co₂ emissions per transport work (mass) [g co₂ / m tonnes · n miles]": "annual_average_co2_emissions_per_transport_work_mass",
    "annual average co₂ emissions per transport work (volume) [g co₂ / m³ · n miles]": "annual_average_co2_emissions_per_transport_work_volume",
    "annual average co₂ emissions per transport work (dwt) [g co₂ / dwt carried · n miles]": "annual_average_co2_emissions_per_transport_work_dwt",
    "annual average co₂ emissions per transport work (pax) [g co₂ / pax · n miles]": "annual_average_co2_emissions_per_transport_work_pax",
    "annual average co₂ emissions per transport work (freight) [g co₂ / m tonnes · n miles]": "annual_average_co2_emissions_per_transport_work_freight",
    "through ice [n miles]": "through_ice",
    "time spent at sea [hours]": "time_spent_at_sea",
    "total time spent at sea through ice [hours]": "total_time_spent_at_sea_through_ice",
    "fuel consumption per distance on laden voyages [kg / n mile]": "fuel_consumption_per_distance_on_laden_voyages",
    "fuel consumption per transport work (mass) on laden voyages [g / m tonnes · n miles]": "fuel_consumption_per_transport_work_mass_on_laden_voyages",
    "fuel consumption per transport work (volume) on laden voyages [g / m³ · n miles]": "fuel_consumption_per_transport_work_volume_on_laden_voyages",
    "fuel consumption per transport work (dwt) on laden voyages [g / dwt carried · n miles]": "fuel_consumption_per_transport_work_dwt_on_laden_voyages",
    "fuel consumption per transport work (pax) on laden voyages [g / pax · n miles]": "fuel_consumption_per_transport_work_pax_on_laden_voyages",
    "fuel consumption per transport work (freight) on laden voyages [g / m tonnes · n miles]": "fuel_consumption_per_transport_work_freight_on_laden_voyages",
    "co₂ emissions per distance on laden voyages [kg co₂ / n mile]": "co2_emissions_per_distance_on_laden_voyages",
    "co₂ emissions per transport work (mass) on laden voyages [g co₂ / m tonnes · n miles]": "co2_emissions_per_transport_work_mass_on_laden_voyages",
    "co₂ emissions per transport work (volume) on laden voyages [g co₂ / m³ · n miles]": "co2_emissions_per_transport_work_volume_on_laden_voyages",
    "co₂ emissions per transport work (dwt) on laden voyages [g co₂ / dwt carried · n miles]": "co2_emissions_per_transport_work_dwt_on_laden_voyages",
    "co₂ emissions per transport work (pax) on laden voyages [g co₂ / pax · n miles]": "co2_emissions_per_transport_work_pax_on_laden_voyages",
    "co₂ emissions per transport work (freight) on laden voyages [g co₂ / m tonnes · n miles]": "co2_emissions_per_transport_work_freight_on_laden_voyages",
    "average density of the cargo transported [m tonnes / m³]": "average_density_of_the_cargo_transported",
    "generation_date": "generation_date",
    "year": "year",
    "version": "version",
    "monitoring_methods": "monitoring_methods",
    "technical_efficiency_type": "technical_efficiency_type",
    "technical_efficiency_value": "technical_efficiency_value",
    "technical_efficiency_unit": "technical_efficiency_unit",
}

spark_df_renamed = spark_df.select(*[col(c).alias(column_mapping.get(c, c)) for c in spark_df.columns])
spark_df_renamed.printSchema()
DyF = DynamicFrame.fromDF(spark_df_renamed, glueContext, "etl_convert")
DyF.printSchema()
# # Apply explicit mapping to avoid Glue Catalog inference issues
# DyF_mapped = ApplyMapping.apply(
#     frame=DyF,
#     mappings=mappings,
#     transformation_ctx="DyF_mapped"
# )
s3output = glueContext.getSink(
  path="s3://eu-marv-ship-emissions/clean",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=["year", "version"],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)
s3output.setCatalogInfo(
  catalogDatabase="ship-emissions-database", catalogTableName="clean_emissions"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(DyF)
job.commit()