import pandas as pd
import numpy as np
import pytest

from pytest_lazyfixture import lazy_fixture

from gzmo.rating.rating_plan import BaseRatingTable, InterpolatedRatingTabe, LookupRatingTable


@pytest.mark.parametrize(
    'table_name, inputs, expected_outputs',
    [
        # string

        # calling without keywords, without and with wildcard
        (
            'rating_table_string',
            'B1',
            {'factor0': 1.2, 'factor1': 2.2}
        ),
        (
            'rating_table_string',
            'X1',
            {'factor0': 2.0, 'factor1': 3.0}
        ),
        # calling with a dict, without and with wildcard
        (
            'rating_table_string',
            {'credit_tier': 'B1'},
            {'factor0': 1.2, 'factor1': 2.2}
        ),
        (
            'rating_table_string',
            {'credit_tier': 'X1'},
            {'factor0': 2.0, 'factor1': 3.0}
        ),
        # calling with a dataframe
        (
            'rating_table_string',
            lazy_fixture('rating_inputs_simple'),
            pd.DataFrame({
                'factor0': [1.4, 1.2, 1.6, 1.8, 2.0],
                'factor1': [2.4, 2.2, 2.6, 2.8, 3.0]
                })
        ),
        
        # boolean

        # calling without keywords, without and with wildcard
        (
            'rating_table_boolean',
            True,
            {'factor0': 1.0, 'factor1': 2.0}
        ),
        (
            'rating_table_boolean',
            False,
            {'factor0': 3.0, 'factor1': 4.0}
        ),
        # calling with a dict, without and with wildcard
        (
            'rating_table_boolean',
            {'safe_driving': True},
            {'factor0': 1.0, 'factor1': 2.0}
        ),
        (
            'rating_table_boolean',
            {'safe_driving': False},
            {'factor0': 3.0, 'factor1': 4.0}
        ),
        # calling with a dataframe
        (
            'rating_table_boolean',
            lazy_fixture('rating_inputs_simple'),
            pd.DataFrame({
                'factor0': [1.0, 1.0, 3.0, 3.0, 3.0],
                'factor1': [2.0, 2.0, 4.0, 4.0, 4.0],
                })
        ),

        # numeric

        # calling without keywords, without and with wildcard
        (
            'rating_table_numeric',
            18, {'factor0': 1.8, 'factor1': 2.8}),
        (
            'rating_table_numeric',
            32, {'factor0': 3.0, 'factor1': 4.0}),    # multiple matches
        (
            'rating_table_numeric',
            0, {'factor0': 3.0, 'factor1': 4.0}),
        # calling with a dict, without and with wildcard
        (
            'rating_table_numeric',
            {'age': 24},
            {'factor0': 2.4, 'factor1': 3.4}
        ),
        (
            'rating_table_numeric',
            {'age': 16},
            {'factor0': 3.0, 'factor1': 4.0}
        ),
        # calling with a dataframe
        (
            'rating_table_numeric',
            lazy_fixture('rating_inputs_simple'),
            pd.DataFrame({
                'factor0': [3.0, 1.8, 2.5, 3.0, 3.0],
                'factor1': [4.0, 2.8, 3.5, 4.0, 4.0]
                })
        ),
        
        # range

        # calling without keywords, without and with wildcard
        (
            'rating_table_range',
            18,
            {'factor0': 1.8, 'factor1': 2.8}),
        (
            'rating_table_range',
            32,
            {'factor0': 2.4, 'factor1': 3.4}),    # multiple matches
        (
            'rating_table_range',
            0,
            {'factor0': 3, 'factor1': 4.0}
        ),
        # calling with a dict, without and with wildcard
        (
            'rating_table_range',
            {'age': 26}, {'factor0': 2.2, 'factor1': 3.2}
        ),
        (
            'rating_table_range',
            {'age': 16}, {'factor0': 3.0, 'factor1': 4.0}
        ),
        # calling with a dataframe
        (
            'rating_table_range',
            lazy_fixture('rating_inputs_simple'),
            pd.DataFrame({
                'factor0': [3.0, 1.8, 2.1, 2.4, 3],
                'factor1': [4.0, 2.8, 3.1, 3.4, 4]
                })
        ),
        
        # combo
        (
            'rating_table_combo',
            {'age': 18, 'safe_driving': False, 'credit_tier': 'C1'},
            {'factor0': 4.5, 'factor1': 5.5}
        ),
        (
            'rating_table_combo',
            lazy_fixture('rating_inputs_simple'),
            pd.DataFrame({
                'factor0': [100, 2.4, 100, 100, 100],
                'factor1': [101, 3.4, 101, 101, 101]
                })
        ),


    ]
)
def test_lookup(rating_table_simple, table_name, inputs, expected_outputs):
    rating_table = LookupRatingTable.from_unprocessed_table(
        rating_table_simple[table_name], 'test_table')
    out = rating_table.evaluate(inputs)
    assert np.all(out == expected_outputs)

@pytest.mark.parametrize(
    'inputs, expected_outputs',
    [

        # calling without keywords
        (
            # below range
            17,
            {'factor0': 1.7, 'factor1': 2.7}),
        (
            # within range; decimal
            18.5,
            {'factor0': 1.85, 'factor1': 2.85}),
        (
            # within range; differnet slope of interpolation
            20.5,
            {'factor0': 2.5, 'factor1': 3.5}),
        (
            # above range
            22,
            {'factor0': 4.0, 'factor1': 5.0}),
        # calling with a dict
        (
            # below range
            {'age': 17},
            {'factor0': 1.7, 'factor1': 2.7}),
        (
            # within range; decimal
            {'age': 18.5},
            {'factor0': 1.85, 'factor1': 2.85}),
        (
            # within range; differnet slope of interpolation
            {'age': 20.5},
            {'factor0': 2.5, 'factor1': 3.5}),
        (
            # above range
            {'age': 22},
            {'factor0': 4.0, 'factor1': 5.0}),
        # calling with a dataframe
        (
            lazy_fixture('rating_inputs_simple'),
            pd.DataFrame({
                'factor0': [0.1, 1.8, 7.0, 452, -0.1],
                'factor1': [1.1, 2.8, 8.0, 453, 0.9]
                })
        ),
    ]
)
def test_interpolate(rating_table_simple, inputs, expected_outputs):
    rating_table = BaseRatingTable.from_unprocessed_table(
        rating_table_simple['rating_table_interpolated'], 'test_table')
    interpolated_rating_table = InterpolatedRatingTabe(rating_table)
    out = interpolated_rating_table.evaluate(inputs)
    if isinstance(expected_outputs, dict):
        for k, v in expected_outputs.items():
            assert np.isclose(out[k], v)
    elif isinstance(expected_outputs, pd.DataFrame):
        print(out)
        print(expected_outputs)
        assert np.allclose(out, expected_outputs)
    else:
        raise TypeError