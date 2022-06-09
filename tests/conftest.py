import pytest
import os
import pandas as pd

@pytest.fixture(scope = 'module')
def rating_table_simple():
    df = pd.read_excel(
        './tests/testdata/test_rating_tables_simple.xlsx',
        true_values = ['True'],
        false_values = ['False'],
        sheet_name = None
        )
    return df

# @pytest.fixture(scope = 'module')
# def rating_inputs_simple():
#     df = pd.read_excel(
#         './tests/testdata/test_portfolio.xlsx',
#         true_values = ['True'],
#         false_values = ['False'],
#         sheet_name = 'rating_inputs_simple'
#         )
#     return df

@pytest.fixture(scope = 'module')
def rating_inputs_simple():
    df = pd.DataFrame({
        'age': [1, 18, 25, 470, -1],
        'safe_driving': [True, True, None, False, 'missing'],
        'credit_tier': ['C1', 'B1', 'D1', 'E1', 'X1']
    })
    return df
