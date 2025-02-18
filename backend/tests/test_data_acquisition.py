import pytest
import pandas as pd

from src.data_acquisition import check_for_new_report_versions
    
def create_test_df(periods, versions, dates, files):
    return pd.DataFrame({
        'Reporting Period': periods,
        'Version': versions,
        'Generation Date': dates,
        'File': files
    })
    
def test_reports_have_new_versions():
    """Tests the check_for_new_report_versions() function if it returns the 
    correct new rows acquired from the website in the current run of the script.

    Args:
        sample_current_df (pd.DataFrame): Pandas dataframe which contains a sample of the metadata from previous runs
        sample_new_df (pd.DataFrame): Pandas dataframe which contains a sample of the metadata from the new run of the script
    """
    sample_current_df = create_test_df(
        periods=[2023, 2022],
        versions=[32, 228],
        dates=['05/02/2025', '29/01/2025'],
        files=['2023-v32-05022025-EU MRV Publication of information', '2022-v228-29012025-EU MRV Publication of information']
    )
    
    sample_new_df = create_test_df(
        periods=[2023, 2022],
        versions=[33, 229],
        dates=['12/02/2025', '12/02/2025'],
        files=['2023-v33-12022025-EU MRV Publication of information', '2022-v229-12022025-EU MRV Publication of information']
    )
    
    new_versions_df = check_for_new_report_versions(current_df=sample_current_df, new_df=sample_new_df)
    assert new_versions_df.iloc[0]['Reporting Period'] == 2023
    assert new_versions_df.iloc[0]['Version_new'] == 33
    assert new_versions_df.iloc[1]['Version_new'] == 229
    
def test_report_version_with_new_year():
    """Tests the check_for_new_report_versions() function if it returns the 
    correct new rows acquired from the website when there is a new year added in the data

    Args:
        sample_current_df (pd.DataFrame): Pandas dataframe which contains a sample of the metadata from previous runs
        sample_new_df (pd.DataFrame): Pandas dataframe which contains a sample of the metadata from the new run of the script
    """
    sample_current_df = create_test_df(
        periods=[2023, 2022],
        versions=[32, 228],
        dates=['05/02/2025', '29/01/2025'],
        files=['2023-v32-05022025-EU MRV Publication of information', '2022-v228-29012025-EU MRV Publication of information']
    )
    
    sample_new_df = create_test_df(
        periods=[2024, 2023, 2022],
        versions=[2, 33, 229],
        dates=['10/02/2025', '12/02/2025', '12/02/2025'],
        files=['2024-v2-10022025-EU MRV Publication of information', '2023-v33-12022025-EU MRV Publication of information', '2022-v229-12022025-EU MRV Publication of information']
    )
    
    new_versions_df = check_for_new_report_versions(current_df=sample_current_df, new_df=sample_new_df)
    assert new_versions_df.iloc[0]['Reporting Period'] == 2024
    assert new_versions_df.iloc[0]['Version_new'] == 2
    
    assert new_versions_df.iloc[1]['Reporting Period'] == 2023
    assert new_versions_df.iloc[1]['Version_new'] == 33


def test_no_changes_to_report_version():
    """Tests the check_for_new_report_versions() function if it returns the 
    correct new rows acquired from the website when there is a new year added in the data

    Args:
        sample_current_df (pd.DataFrame): Pandas dataframe which contains a sample of the metadata from previous runs
        sample_new_df (pd.DataFrame): Pandas dataframe which contains a sample of the metadata from the new run of the script
    """
    sample_current_df = create_test_df(
        periods=[2023, 2022],
        versions=[33, 229],
        dates=['12/02/2025', '12/02/2025'],
        files=['2023-v33-12022025-EU MRV Publication of information', '2022-v229-12022025-EU MRV Publication of information']
    )
    
    sample_new_df = create_test_df(
        periods=[2023, 2022],
        versions=[33, 229],
        dates=['12/02/2025', '12/02/2025'],
        files=['2023-v33-12022025-EU MRV Publication of information', '2022-v229-12022025-EU MRV Publication of information']
    )
    
    new_versions_df = check_for_new_report_versions(current_df=sample_current_df, new_df=sample_new_df)
    assert new_versions_df == create_test_df(periods=[], versions=[], dates=[], files=[])