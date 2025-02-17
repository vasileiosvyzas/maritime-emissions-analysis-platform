import pytest
import pandas as pd

from src.data_acquisition import check_for_new_report_versions

@pytest.fixture
def sample_current_df():
    return pd.DataFrame({
        'Reporting Period': [2023, 2022],
        'Version': [32, 228],
        'Generation Date': ['05/02/2025', '29/01/2025'],
        'File': ['2023-v32-05022025-EU MRV Publication of information', '2022-v228-29012025-EU MRV Publication of information']
    })
    
@pytest.fixture
def sample_new_df():
    return pd.DataFrame({
        'Reporting Period': [2023, 2022],
        'Version': [33, 229],
        'Generation Date': ['12/02/2025', '12/02/2025'],
        'File': ['2023-v33-12022025-EU MRV Publication of information', '2022-v229-12022025-EU MRV Publication of information']
    })
    
def test_check_for_new_report_versions(sample_current_df:pd.DataFrame, sample_new_df:pd.DataFrame):
    """Tests the check_for_new_report_versions() function if it returns the 
    correct new rows acquired from the website in the current run of the script.

    Args:
        sample_current_df (pd.DataFrame): Pandas dataframe which contains a sample of the metadata from previous runs
        sample_new_df (pd.DataFrame): Pandas dataframe which contains a sample of the metadata from the new run of the script
    """
    new_versions_df = check_for_new_report_versions(current_df=sample_current_df, new_df=sample_new_df)
    assert new_versions_df['Version_new'] == pd.Series([33, 229])
    assert new_versions_df['File_new'] == pd.Series(['2023-v32-05022025-EU MRV Publication of information', '2022-v228-29012025-EU MRV Publication of information'])