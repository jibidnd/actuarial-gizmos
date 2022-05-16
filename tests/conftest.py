import pytest
import os
import pandas as pd

@pytest.fixture(scope = 'module')
def rating_table_simple():
    print(os.getcwd())
    df = pd.read_excel('./tests/testdata/test_rating_tables_simple.xlsx',
        sheet_name = None)
    return df