import pytest
from unittest.mock import patch, Mock
import pandas as pd
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

def test_extract(etl_pipeline):
    """Test the extract method of ETLPipeline."""
    # Setup mock blobs
    mock_blob1 = Mock()
    mock_blob1.name = 'bronze-bucket/2023/report1.xlsx'
    mock_blob1.metadata = {'processed_by_ETL': 'False'}
    
    mock_blob2 = Mock()
    mock_blob2.name = 'bronze-bucket/2023/report2.xlsx'
    mock_blob2.metadata = {'processed_by_ETL': 'True'}  # Already processed
    
    # Setup mock list_blobs
    etl_pipeline.storage_client.client.list_blobs.return_value = [mock_blob1, mock_blob2]
    
    # Create sample data
    sample_df = pd.DataFrame({'A': [1, 2], 'B': ['a', 'b']})
    etl_pipeline.storage_client.download_file_into_memory.return_value = sample_df
    
    # Call the method
    result = etl_pipeline.extract()
    
    # Assertions
    assert len(result) == 1
    assert 'bronze-bucket/2023/report1.xlsx' in result
    etl_pipeline.storage_client.download_file_into_memory.assert_called_once_with(
        blob_name='2023/report1.xlsx', bucket_layer='bronze-bucket'
    )
    pd.testing.assert_frame_equal(result['bronze-bucket/2023/report1.xlsx'], sample_df)

def test_transform(etl_pipeline, sample_data):
    """Test the transform method of ETLPipeline."""
    # Call the transform method
    result = etl_pipeline.tranform(sample_data.copy())
    
    # Assertions for columns that should be removed
    assert 'D.1' not in result.columns
    assert 'Additional information to facilitate the understanding of the reported average operational energy efficiency indicators' not in result.columns
    assert 'Technical efficiency' not in result.columns
    
    # Assertions for new columns
    assert 'technical_efficiency_type' in result.columns
    assert 'technical_efficiency_value' in result.columns
    assert 'technical_efficiency_unit' in result.columns
    assert 'monitoring_methods' in result.columns
    
    # Assertions for removed monitoring method columns
    assert 'A' not in result.columns
    assert 'B' not in result.columns
    assert 'C' not in result.columns
    assert 'D' not in result.columns
    
    # Check column renaming
    assert 'imo_number' in result.columns
    assert 'name' in result.columns
    assert 'ship_type' in result.columns
    
    # Check technical efficiency extraction
    assert result['technical_efficiency_type'].iloc[0] == 'EEDI'
    assert result['technical_efficiency_value'].iloc[0] == 5.2
    assert result['technical_efficiency_unit'].iloc[0] == 'g/t-nm'
    
    # Check date conversion
    assert pd.isna(result['doc_issue_date'].iloc[1])
    assert pd.isna(result['doc_expiry_date'].iloc[1])
    assert result['doc_issue_date'].iloc[0].date() == datetime.date(2023, 1, 1)
    
    # Check monitoring methods
    assert result['monitoring_methods'].iloc[0] == 'A, C'
    assert result['monitoring_methods'].iloc[1] == 'B, C'

def test_load(etl_pipeline):
    """Test the load method of ETLPipeline."""
    df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    report_name = '2023/report.parquet'
    bucket_layer = 'silver-bucket'
    
    # Setup mock blob
    mock_blob = Mock()
    etl_pipeline.storage_client.bucket.get_blob.return_value = mock_blob
    
    # Call the method
    etl_pipeline.load(df, report_name, bucket_layer)
    
    # Assertions
    etl_pipeline.storage_client.upload_parquet_file_to_bucket.assert_called_once_with(
        bucket_layer=bucket_layer, 
        dataframe=df, 
        destination_blob_name=report_name
    )
    
    etl_pipeline.storage_client.bucket.get_blob.assert_called_once_with(f"{bucket_layer}/{report_name}")
    
    assert mock_blob.metadata['processed_by_ETL'] is True
    assert 'processed_date' in mock_blob.metadata
    mock_blob.patch.assert_called_once()

def test_run(etl_pipeline):
    """Test the run method of ETLPipeline."""
    # Setup mocks for individual methods
    with patch.object(etl_pipeline, 'extract') as mock_extract, \
         patch.object(etl_pipeline, 'tranform') as mock_transform, \
         patch.object(etl_pipeline, 'load') as mock_load:
        
        # Setup return values
        mock_df = pd.DataFrame({'test': [1, 2]})
        mock_transformed_df = pd.DataFrame({'processed': [3, 4]})
        
        mock_extract.return_value = {'bronze-bucket/2023/report.xlsx': mock_df}
        mock_transform.return_value = mock_transformed_df
        
        # Setup blob mock for metadata update
        mock_blob = Mock()
        etl_pipeline.storage_client.bucket.get_blob.return_value = mock_blob
        
        # Call the run method
        etl_pipeline.run()
        
        # Assertions
        mock_extract.assert_called_once()
        mock_transform.assert_called_once_with(df=mock_df)
        mock_load.assert_called_once_with(
            clean_dataframe=mock_transformed_df, 
            report_name='2023/report.parquet', 
            bucket_layer='silver-bucket'
        )
        
        etl_pipeline.storage_client.bucket.get_blob.assert_called_once_with('bronze-bucket/2023/report.xlsx')
        
        assert mock_blob.metadata['processed_by_ETL'] is True
        assert 'processed_date' in mock_blob.metadata
        mock_blob.patch.assert_called_once()