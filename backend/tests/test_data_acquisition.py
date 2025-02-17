import pytest
import pandas as pd

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