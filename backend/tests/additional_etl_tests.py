import pytest
from unittest.mock import patch, Mock
import pandas as pd
import numpy as np
import datetime
from src.etl_pipeline import ETLPipeline

@pytest.fixture
def etl_pipeline():
    """Fixture that creates an ETL pipeline with mocked storage manager."""
    with patch('src.etl_pipeline.GoogleCloudStorageManager') as mock_storage_manager:
        etl = ETLPipeline()
        yield etl

@pytest.fixture
def sample_data():
    """Fixture that creates sample data for testing transformations."""
    return pd.DataFrame({
        'IMO Number': [1234567, 7654321],
        'Name': ['Ship A', 'Ship B'],
        'Ship type': ['Cargo', 'Passenger'],
        'Technical efficiency': ['EEDI (5.2 g/t-nm)', 'EEDI (4.8 g/t-nm)'],
        'Reporting Period': [2023, 2023],
        'Port of Registry': ['Port A', 'Port B'],
        'Home Port': ['Home A', 'Home B'],
        'Ice Class': ['1A', '1B'],
        'A': ['Yes', 'No'],
        'B': ['No', 'Yes'],
        'C': ['Yes', 'Yes'],
        'D': ['No', 'No'],
        'DoC issue date': ['01/01/2023', 'DoC not issued'],
        'DoC expiry date': ['01/01/2025', 'DoC not issued'],
        'Verifier Number': ['V123', 'V456'],
        'Verifier Address': ['1 Place Zaha Hadid, 92400 COURBEVOIE, France', '16, Efplias Str, 185 37 Piraeus, Greece'],
        'Verifier Name': ['Verifier A', 'Verifier B'],
        'Verifier NAB': ['NAB1', 'NAB2'],
        'Verifier City': ['City A', 'City B'],
        'Verifier Accreditation number': ['ACC123', 'ACC456'],
        'Verifier Country': ['Country A', 'Country B'],
        'Total fuel consumption [m tonnes]': [100.5, 150.2],
        'Fuel consumptions assigned to On laden [m tonnes]': [80.3, 120.1],
        'Total CO₂ emissions [m tonnes]': [300.6, 450.9],
        'Annual Time spent at sea [hours]': [8000, 7500],
        # Columns to be dropped
        'D.1': ['drop1', 'drop2'],
        'Additional information to facilitate the understanding of the reported average operational energy efficiency indicators': ['info1', 'info2']
    })
    
# Data transformation specific tests
@pytest.mark.parametrize("tech_eff, expected_type, expected_value, expected_unit", [
    ('EEDI (5.2 g/t-nm)', 'EEDI', 5.2, 'g/t-nm'),
    ('EIV (7.8 g/dwt-nm)', 'EIV', 7.8, 'g/dwt-nm'),
    ('Missing', 'Missing', np.nan, np.nan),
    (None, 'Missing', np.nan, np.nan)
])
def test_technical_efficiency_extraction(etl_pipeline, tech_eff, expected_type, expected_value, expected_unit):
    """Test extraction of technical efficiency data with different inputs."""
    data = pd.DataFrame({
        'Technical efficiency': [tech_eff]
    })
    
    result = etl_pipeline.tranform(data.copy())
    
    if pd.isna(expected_type):
        assert pd.isna(result['technical_efficiency_type'].iloc[0])
    else:
        assert result['technical_efficiency_type'].iloc[0] == expected_type
        
    if pd.isna(expected_value):
        assert pd.isna(result['technical_efficiency_value'].iloc[0])
    else:
        assert result['technical_efficiency_value'].iloc[0] == expected_value
        
    if pd.isna(expected_unit):
        assert pd.isna(result['technical_efficiency_unit'].iloc[0])
    else:
        assert result['technical_efficiency_unit'].iloc[0] == expected_unit

@pytest.mark.parametrize("a, b, c, d, expected", [
    ('Yes', 'Yes', 'Yes', 'Yes', 'A, B, C, D'),
    ('Yes', 'No', 'Yes', 'No', 'A, C'),
    ('No', 'Yes', 'No', 'Yes', 'B, D'),
    ('No', 'No', 'No', 'No', '')
])
def test_monitoring_methods_generation(etl_pipeline, a, b, c, d, expected):
    """Test generation of monitoring methods string with various combinations."""
    data = pd.DataFrame({
        'A': [a],
        'B': [b],
        'C': [c],
        'D': [d]
    })
    
    result = etl_pipeline.tranform(data.copy())
    
    assert result['monitoring_methods'].iloc[0] == expected

@pytest.mark.parametrize("issue_date, expiry_date, expected_issue_date, expected_expiry_date", [
    ('01/01/2023', '31/12/2024', datetime.date(2023, 1, 1), datetime.date(2024, 12, 31)),
    ('DoC not issued', '30/09/2025', None, datetime.date(2025, 9, 30)),
    ('15/06/2022', 'DoC not issued', datetime.date(2022, 6, 15), None),
    (None, None, None, None)
])
def test_date_conversion(etl_pipeline, issue_date, expiry_date, expected_issue_date, expected_expiry_date):
    """Test date conversion with various inputs."""
    data = pd.DataFrame({
        'DoC issue date': [issue_date],
        'DoC expiry date': [expiry_date]
    })
    
    result = etl_pipeline.tranform(data.copy())
    
    if expected_issue_date is None:
        assert pd.isna(result['doc_issue_date'].iloc[0])
    else:
        assert result['doc_issue_date'].iloc[0].date() == expected_issue_date
        
    if expected_expiry_date is None:
        assert pd.isna(result['doc_expiry_date'].iloc[0])
    else:
        assert result['doc_expiry_date'].iloc[0].date() == expected_expiry_date

def test_na_handling_for_object_columns(etl_pipeline):
    """Test handling of NA values in object columns."""
    data = pd.DataFrame({
        'Name': ['Ship A', None, np.nan],
        'Ship type': [np.nan, 'Cargo', None],
        'Technical efficiency': ['EEDI (5.2 g/t-nm)', None, np.nan],
        'Port of Registry': ['Port A', np.nan, None]
    })
    
    result = etl_pipeline.tranform(data.copy())
    
    # Check NA replacement with 'Missing'
    assert result['name'].iloc[0] == 'Ship A'
    assert result['name'].iloc[1] == 'Missing'
    assert result['name'].iloc[2] == 'Missing'
    
    assert result['ship_type'].iloc[0] == 'Missing'
    assert result['ship_type'].iloc[1] == 'Cargo'
    assert result['ship_type'].iloc[2] == 'Missing'
    
    assert result['port_of_registry'].iloc[0] == 'Port A'
    assert result['port_of_registry'].iloc[1] == 'Missing'
    assert result['port_of_registry'].iloc[2] == 'Missing'