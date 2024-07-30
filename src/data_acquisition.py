import logging
import os
import time
import traceback
import boto3

import pandas as pd

from sys import stdout
from botocore.exceptions import ClientError
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

logger = logging.getLogger('mylogger')

logger.setLevel(logging.DEBUG) # set logger level
logFormatter = logging.Formatter\
("%(name)-12s %(asctime)s %(levelname)-8s %(filename)s:%(funcName)s %(message)s")
consoleHandler = logging.StreamHandler(stdout) #set streamhandler to stdout
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)


def prepare_selenium_params():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_experimental_option("prefs", {"download.default_directory": "/tmp"})
    driver = webdriver.Remote("http://chrome:4444", options=options)

    return driver


def get_reporting_table_content():
    """
    This function gets the metadata from the table in the website that contains
    the links to the excel reports

    Returns: a Pandas dataframe with the columns: ['Reporting Period', 'Version', 'Generation Date', 'File']. It
    writes the dataframe on disk
    """

    driver = prepare_selenium_params()

    try:
        logger.info("Visiting the Thetis MRV website")

        driver.get("https://mrv.emsa.europa.eu/#public/emission-report")
        time.sleep(30)

        table_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="gridview-1152"]/div[2]'))
        )
        table_rows = table_element.find_elements(By.TAG_NAME, "tr")
        table_data = []
        for row in table_rows:
            row_data = []
            cells = row.find_elements(By.TAG_NAME, "td")
            for cell in cells:
                row_data.append(cell.text)
            table_data.append(row_data)
        
        logger.info("Got the table data extract")
        logger.info(table_data)
        result = []
        
        logger.info('Converting the data to a dataframe')
        for row in table_data:
            data = {}
            data["Reporting Period"] = row[1].split("Reporting Period")[1]
            data["Version"] = row[2].split("Version")[1]
            data["Generation Date"] = row[3].split("Generation Date")[1]
            data["File"] = row[4].split("File")[1]
            result.append(data)
        new_report_table_data_df = pd.DataFrame(result)

        logger.info('Got the new dataframe')
        logger.info(new_report_table_data_df.head())

        return new_report_table_data_df

    except Exception as e:
        logger.error(f"An error occurred while getting the data: {e}")
        logger.error(traceback.format_exc())
    finally:
        driver.quit()


def download_new_file(report):
    """
    This function gets called to download the new file from the website
    It uses Selenium to click on the new link text

    """
    driver = prepare_selenium_params()

    driver.get("https://mrv.emsa.europa.eu/#public/emission-report")
    time.sleep(20)

    try:
        logger.info(f"Downloading the new report: {report}")

        wait = WebDriverWait(driver, 30)
        link = wait.until(EC.presence_of_element_located((By.LINK_TEXT, report)))
        link.click()
        time.sleep(20)

        logger.info(f"File {report} is downloaded")

    except Exception as e:
        logger.error(f"An error occurred while getting the data: {e}")
        logger.error(traceback.format_exc())
    finally:
        driver.quit()


def check_for_new_report_versions(current_df, new_df):
    """Takes the dataframe with the table data extracted at current run and the dataframe with the table data 
    from the previous run and compares them to check for new versions of the report

    Args:
        current_df (DataFrame): contains the data from the previous run
        new_df (DataFrame): contains the data from the current run
    """
    logger.info("Comparing the versions in the current and new dataframes")

    merged_df = pd.merge(
        current_df, new_df, on="Reporting Period", how="right", suffixes=("_current", "_new")
    ).fillna(0)
    new_versions = merged_df[merged_df["Version_new"] > merged_df["Version_current"]]
    
    return new_versions
    

def delete_file_from_local_directory(filepath):
    if os.path.exists(filepath):
        logger.info(f"Deleting the file from the local path: {filepath}")
        os.remove(filepath)
    else:
        logger.info("The file does not exist so I couldn't delete it")


def fix_column_types(df):
    logger.info("Convering the column types of the dataframe")

    df["Reporting Period"] = df[["Reporting Period"]].astype(int)
    df["Version"] = df[["Version"]].astype(int)
    df["Generation Date"] = pd.to_datetime(df["Generation Date"], dayfirst=True)

    return df


def upload_file(file_name, bucket, object_name=None):
    """
    Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    logger.info("Starting the upload to S3")
    
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    s3_client = boto3.client(
        "s3", 
        region_name="us-east-1"
    )

    try:
        logger.info(
            f"Uploading the file: {file_name} to the bucket: {bucket} with object name: {object_name}"
        )

        s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def main():
    logger.info("Getting the new metadata from the reports table")
    reports_df_new = get_reporting_table_content()
    reports_df_new = fix_column_types(reports_df_new)

    logger.info("New metadata from the website")
    logger.info(reports_df_new.head())
    logger.info(reports_df_new.dtypes)
    
    print(os.getcwd())

    reports_df_old = pd.read_csv("reports_metadata.csv")

    logger.info("Report versions from previous run")
    logger.info(reports_df_old.head())
    
    logger.info('Check for new versions of the reports')
    df_with_new_versions = check_for_new_report_versions(current_df=reports_df_old, new_df=reports_df_new)

    download_directory = "/tmp"
    
    if not df_with_new_versions.empty:
        new_files = df_with_new_versions['File_new'].to_list()
        logger.info(f"Found {len(new_files)} new files that need to be downloaded")
        
        for new_file_name in new_files:
            new_file_name = new_file_name.strip()
            download_new_file(report=new_file_name)
            
            filepath = f"{download_directory}/{new_file_name}.xlsx"
            year = new_file_name.split('-')[0]
            filename = f"{new_file_name}.xlsx"
            
            upload_file(
                file_name=filepath,
                bucket="eu-marv-ship-emissions",
                object_name=f"raw/{year}/{filename}",
            )

            delete_file_from_local_directory(filepath=filepath)
    
    reports_df_updated = df_with_new_versions[['Reporting Period', 'Version_new', 'Generation Date_new', 'File_new']].copy()
    reports_df_updated.rename(columns={'Version_new': 'Version', 'Generation Date_new': 'Generation Date', 'File_new': 'File'}, inplace=True)
    logger.info('Updated the current df')
    
    reports_df_updated.to_csv("reports_metadata.csv", index=False)


if __name__ == "__main__":
    main()
