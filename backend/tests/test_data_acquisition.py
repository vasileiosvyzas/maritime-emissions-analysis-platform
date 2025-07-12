import pytest
import pandas as pd

from unittest.mock import patch, MagicMock
from selenium.common.exceptions import TimeoutException
from src.data_acquisition import check_for_new_report_versions, get_reporting_table_content, prepare_selenium_params


def create_test_df(periods, versions, dates, files):
    return pd.DataFrame(
        {
            "Reporting Period": periods,
            "Version": versions,
            "Generation Date": dates,
            "File": files,
        }
    )


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
        dates=["05/02/2025", "29/01/2025"],
        files=[
            "2023-v32-05022025-EU MRV Publication of information",
            "2022-v228-29012025-EU MRV Publication of information",
        ],
    )

    sample_new_df = create_test_df(
        periods=[2023, 2022],
        versions=[33, 229],
        dates=["12/02/2025", "12/02/2025"],
        files=[
            "2023-v33-12022025-EU MRV Publication of information",
            "2022-v229-12022025-EU MRV Publication of information",
        ],
    )

    new_versions_df, new_reports = check_for_new_report_versions(
        current_df=sample_current_df, new_df=sample_new_df
    )
    assert new_versions_df.iloc[0]["Reporting Period"] == 2023
    assert new_versions_df.iloc[0]["Version"] == 33
    assert new_versions_df.iloc[1]["Version"] == 229
    
    assert new_reports.shape == (2, 4)


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
        dates=["05/02/2025", "29/01/2025"],
        files=[
            "2023-v32-05022025-EU MRV Publication of information",
            "2022-v228-29012025-EU MRV Publication of information",
        ],
    )

    sample_new_df = create_test_df(
        periods=[2024, 2023, 2022],
        versions=[2, 33, 229],
        dates=["10/02/2025", "12/02/2025", "12/02/2025"],
        files=[
            "2024-v2-10022025-EU MRV Publication of information",
            "2023-v33-12022025-EU MRV Publication of information",
            "2022-v229-12022025-EU MRV Publication of information",
        ],
    )

    new_versions_df, new_reports = check_for_new_report_versions(
        current_df=sample_current_df, new_df=sample_new_df
    )
    assert new_versions_df.iloc[0]["Reporting Period"] == 2024
    assert new_versions_df.iloc[0]["Version"] == 2

    assert new_versions_df.iloc[1]["Reporting Period"] == 2023
    assert new_versions_df.iloc[1]["Version"] == 33
    
    assert new_reports.shape == (3, 4)


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
        dates=["12/02/2025", "12/02/2025"],
        files=[
            "2023-v33-12022025-EU MRV Publication of information",
            "2022-v229-12022025-EU MRV Publication of information",
        ],
    )

    sample_new_df = create_test_df(
        periods=[2023, 2022],
        versions=[33, 229],
        dates=["12/02/2025", "12/02/2025"],
        files=[
            "2023-v33-12022025-EU MRV Publication of information",
            "2022-v229-12022025-EU MRV Publication of information",
        ],
    )

    new_versions_df, new_reports = check_for_new_report_versions(
        current_df=sample_current_df, new_df=sample_new_df
    )
    assert new_versions_df.empty == True
    assert new_reports.shape == (2, 4)

# Test for successful data extraction
@patch('src.data_acquisition.prepare_selenium_params')
@patch('src.data_acquisition.WebDriverWait')
@patch('src.data_acquisition.time.sleep')
def test_successful_data_extraction(mock_sleep, mock_wait, mock_prepare_selenium):
    # Setup mock driver
    mock_driver = MagicMock()
    mock_prepare_selenium.return_value = mock_driver
    
    # Setup mock table element and rows
    mock_table = MagicMock()
    mock_wait.return_value.until.return_value = mock_table
    
    # Create mock rows
    mock_rows = []
    test_data = [
        ["", "Reporting Period2023", "Version1.0", "Generation Date2023-04-01", "FileReport-2023-v1"],
        ["", "Reporting Period2022", "Version2.1", "Generation Date2022-04-02", "FileReport-2022-v2.1"],
        ["", "Reporting Period2021", "Version1.5", "Generation Date2021-04-03", "FileReport-2021-v1.5"]
    ]
    
    for row_data in test_data:
        mock_row = MagicMock()
        mock_cells = []
        
        for cell_value in row_data:
            mock_cell = MagicMock()
            mock_cell.text = cell_value
            mock_cells.append(mock_cell)
        
        mock_row.find_elements.return_value = mock_cells
        mock_rows.append(mock_row)
    
    mock_table.find_elements.return_value = mock_rows
    
    # Expected DataFrame
    expected_df = pd.DataFrame([
        {"Reporting Period": "2023", "Version": "1.0", "Generation Date": "2023-04-01", "File": "Report-2023-v1"},
        {"Reporting Period": "2022", "Version": "2.1", "Generation Date": "2022-04-02", "File": "Report-2022-v2.1"},
        {"Reporting Period": "2021", "Version": "1.5", "Generation Date": "2021-04-03", "File": "Report-2021-v1.5"}
    ])
    
    # Call the function
    result_df = get_reporting_table_content()
    
    # Assertions
    mock_prepare_selenium.assert_called_once()
    mock_driver.get.assert_called_once_with("https://mrv.emsa.europa.eu/#public/emission-report")
    mock_sleep.assert_called_once_with(30)
    mock_wait.assert_called_once()
    mock_driver.quit.assert_called_once()
    
    # Check DataFrame values
    assert result_df.shape == expected_df.shape
    pd.testing.assert_frame_equal(result_df, expected_df)
    
    

# Test for timeout exception during element wait
@patch('src.data_acquisition.prepare_selenium_params')
@patch('src.data_acquisition.WebDriverWait')
def test_exception_during_table_wait(mock_wait, mock_prepare_selenium):
    # Setup mock driver
    mock_driver = MagicMock()
    mock_prepare_selenium.return_value = mock_driver
    
    # Setup WebDriverWait to raise a timeout exception
    mock_wait.return_value.until.side_effect = TimeoutException("Element not found")
    
    # Call the function
    result = get_reporting_table_content()
    
    # Assertions
    mock_prepare_selenium.assert_called_once()
    mock_driver.get.assert_called_once()
    mock_wait.assert_called_once()
    mock_driver.quit.assert_called_once()
    assert result is None