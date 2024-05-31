import os
import urllib.parse

import awswrangler as wr

os_input_s3_cleansed_layer = os.environ["s3_cleansed_layer"]
os_input_glue_catalog_db_name = os.environ["glue_catalog_db_name"]
os_input_glue_catalog_table_name = os.environ["glue_catalog_table_name"]
os_input_write_data_operation = os.environ["write_data_operation"]


def lambda_handler(event, context):
    print(event)
    # Get the object from the event and show its content type
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(event["Records"][0]["s3"]["object"]["key"], encoding="utf-8")

    try:

        # Creating DF from content
        df_raw = wr.s3.read_excel("s3://{}/{}".format(bucket, key), engine="openpyxl", header=2)
        print(df_raw.head())
        print(df_raw.shape)

        # Extract required columns:
        df_small = df_raw[
            [
                "IMO Number",
                "CO₂ emissions from all voyages which departed from ports under a MS jurisdiction [m tonnes]",
                "Total fuel consumption [m tonnes]",
                "CO₂ emissions from all voyages to ports under a MS jurisdiction [m tonnes]",
                "CO₂ emissions which occurred within ports under a MS jurisdiction at berth [m tonnes]",
                "Verifier Country",
                "Verifier Accreditation number",
                "Verifier City",
                "Verifier Address",
                "Total CO₂ emissions [m tonnes]",
                "Verifier NAB",
                "DoC expiry date",
                "DoC issue date",
                "Reporting Period",
                "Ship type",
                "Name",
                "Verifier Name",
                "CO₂ emissions from all voyages between ports under a MS jurisdiction [m tonnes]",
                "Technical efficiency",
                "Port of Registry",
            ]
        ]

        # # Write to S3
        wr_response = wr.s3.to_parquet(
            df=df_small,
            path=os_input_s3_cleansed_layer,
            dataset=True,
            database=os_input_glue_catalog_db_name,
            table=os_input_glue_catalog_table_name,
            mode=os_input_write_data_operation,
        )

        return wr_response
    except Exception as e:
        print(e)
        print(
            "Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.".format(
                key, bucket
            )
        )
        raise e
