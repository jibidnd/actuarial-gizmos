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

@pytest.fixture(scope = 'module')
def rating_inputs_simple():
    df = pd.read_excel(
        './tests/testdata/test_rating_inputs_simple.xlsx'
        )
    return df

@pytest.fixture(scope = 'module')
def portfolio_simple():
    df = pd.read_excel(
        './tests/testdata/test_portfolio_simple.xlsx',
        sheet_name = None
        )
    return df

@pytest.fixture(scope = 'module')
def portfolio_simple_indexed(portfolio_simple):
    portfolio_simple['policy_info'] \
        .set_index(['policy_number'], inplace = True)
    portfolio_simple['driver_info'] \
        .set_index(['policy_number', 'driver_number'], inplace = True)
    portfolio_simple['vehicle_info'] \
        .set_index(['policy_number', 'vehicle_number'], inplace = True)
    portfolio_simple['driver_claims'] \
        .set_index(['license_number', 'violation_date'], inplace = True)
    return portfolio_simple

# @pytest.fixture(scope = 'module')
# def rating_inputs_simple():
#     df = pd.DataFrame({
#         'age': [1, 18, 25, 470, -1],
#         'safe_driving': [True, True, None, False, 'missing'],
#         'credit_tier': ['C1', 'B1', 'D1', 'E1', 'X1']
#     })
#     return df
