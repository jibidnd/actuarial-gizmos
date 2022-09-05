import pandas as pd
import numpy as np
import pytest

from pytest_lazyfixture import lazy_fixture

from gzmo.base import SearchableDict
from gzmo.helpers import set_unique_index

def test_fails_nonunique_index():
    # passing dataframes with non-unique indices should raise
    df_nonunique_idx = pd.DataFrame(
        data = {'testdata': [2,2,2]},
        index = [1,1,1]
    )
    # indices with no names are automatically dropped/reset
    df_nonunique_idx.index.name = 'idx'
    with pytest.raises(
        Exception,
        match = f'Unable to set unique index for dataframe testdf'
        ):
        book = SearchableDict(testdf = df_nonunique_idx)
    
def test_set_joinable_indices(portfolio_simple_indexed):
    # this tests the Book.set_joinable_index function
    # any column that appears in another dataframe should be in the index
    # initialize the book
    book = SearchableDict(**portfolio_simple_indexed)
    # none of policy_info's other columns appear in other dataframes
    assert book['policy_info'].index.names == \
        ['policy_number']
    # license_number appear in driver_claims, so should be added to the index
    assert book['driver_info'].index.names == \
        ['policy_number', 'driver_number', 'license_number']
    # none of vehicle_info's other columns appear in other dataframes
    assert book['vehicle_info'].index.names == \
        ['policy_number', 'vehicle_number']
    # none of driver_claims' other columns appear in other dataframes
    assert book['driver_claims'].index.names == \
        ['license_number', 'violation_date']

@pytest.mark.parametrize(
    'table_name, expected_index',
    [
        (
            'policy_info',
            ['policy_number']
        ),
        (
            'driver_info',
            ['policy_number', 'driver_number']
        ),
        (
            'vehicle_info',
            ['policy_number', 'vehicle_number']
        ),
        (
            'driver_claims',
            ['license_number', 'violation_date']
        )
    ]
)
def test_set_unique_index(portfolio_simple, table_name, expected_index):
    # this tests the Book.set_unique_indices function
    tempdf = portfolio_simple[table_name]
    out_df = set_unique_index(tempdf, max_cols = 5)
    assert out_df.index.names == expected_index

def test_autojoin_left(portfolio_simple_indexed):
    book = SearchableDict(**portfolio_simple_indexed)
    lst_columns = [
        'policy_number', 'credit_tier',         # policy level
        'driver_number', 'license_number',      # driver level
        'violation_date', 'violation_code']     # claim level
    joined = book.get(lst_columns, how = 'left')

    # check that we got everything we asked for
    assert all(joined.columns == lst_columns)

    # check that it is indexed correctly
    assert joined.index.names == \
        ['policy_number', 'driver_number', 'license_number', 'violation_date']
    
    # check that there are the correct number of rows
    # L0001's 3 claims should be joined on 2 terms each   = 6 rows
    # L0002's 1 claim should be joined on 2 terms each    = 2 rows
    # L0003's 2 claims should be joined on 2 term each    = 4 rows
    # L0004 does not have any claims, but should have       1 row
    #   since this is a left join
    # For a total of 13 rows
    print(joined.index)
    assert len(joined) == 13

def test_autojoin_inner(portfolio_simple_indexed):
    book = SearchableDict(**portfolio_simple_indexed)
    lst_columns = [
        'policy_number', 'credit_tier',         # policy level
        'driver_number', 'license_number',      # driver level
        'violation_date', 'violation_code']     # claim level
    joined = book.get(lst_columns, how = 'inner')

    # check that we got everything we asked for
    assert all(joined.columns == lst_columns)

    # check that it is indexed correctly
    assert joined.index.names == \
        ['policy_number', 'driver_number', 'license_number', 'violation_date']
    
    # check that there are the correct number of rows
    # L0001's 3 claims should be joined on 2 terms each   = 6 rows
    # L0002's 1 claim should be joined on 2 terms each    = 2 rows
    # L0003's 2 claims should be joined on 2 term each    = 4 rows
    # L0004 does not have any claims, and should have       0 row
    #   since this is an inner join
    # For a total of 13 rows
    assert len(joined) == 12