from pickle import TRUE
import pandas as pd
import numpy as np
import pytest
from regex import R

from gzmo.rating.rating_plan import RatingTable

# Fixtures
# =====================================================================
@pytest.fixture
def test_input_simple():
    df = pd.DataFrame({
        'age': [1, 18, 25, 21, 10000],
        'safe_driving': [True, True, False, False, 'missing'],
        'credit_tier': ['B1', 'C1', 'D1', 'E1', 'X1']
    })
    return df


# Simple call of the rating table with 1 or two sets of inputs
# =====================================================================
# No wildcards
# ---------------------------------------------------------------------
# @pytest.mark.parametrize(
#     'inputs, expected_outputs',
#     [
#         ([[1]], np.array([[np.nan]])),
#         ([[18]], np.array([[1.8]])),
#         ([[19], [20]], np.array([[1.9], [2.0]]))
#     ]
#     )
# def test_rating_table_numeric_call(rating_table_simple, inputs, expected_outputs):
#     rating_table = RatingTable(
#         'test_table', rating_table_simple['rating_table_numeric'])
#     print(rating_table)
#     out = rating_table(inputs)
#     print(rating_table)
#     assert np.allclose(out, expected_outputs, equal_nan = True)

# @pytest.mark.parametrize(
#     'inputs, expected_outputs',
#     [
#         ([[1]], np.array([[np.nan]])),
#         ([[18]], np.array([[1.8]])),
#         ([[27], [60]], np.array([[2.0], [2.4]]))
#     ]
#     )
# def test_rating_table_range_call(rating_table_simple, inputs, expected_outputs):
#     rating_table = RatingTable(
#         'test_table', rating_table_simple['rating_table_range'])
#     out = rating_table(inputs)
#     assert np.allclose(out, expected_outputs, equal_nan = True)

# @pytest.mark.parametrize(
#     'inputs, expected_outputs',
#     [
#         ([['no_match']], np.array([[np.nan]])),
#         ([[True]], np.array([[1.0]])),
#         ([[True], [False]], np.array([[1.0], [1.5]]))
#     ]
#     )
# def test_rating_table_boolean_call(rating_table_simple, inputs, expected_outputs):
#     rating_table = RatingTable(
#         'test_table', rating_table_simple['rating_table_boolean'])
#     out = rating_table(inputs)
#     assert np.allclose(out, expected_outputs, equal_nan = True)

# @pytest.mark.parametrize(
#     'inputs, expected_outputs',
#     [
#         ([['X12345']], np.array([[np.nan]])),
#         ([['A1']], np.array([[1.0]])),
#         ([['A1'], ['E1']], np.array([[1.0], [1.8]]))
#     ]
#     )
# def test_rating_table_str_call(rating_table_simple, inputs, expected_outputs):
#     rating_table = RatingTable(
#         'test_table', rating_table_simple['rating_table_string'])
#     out = rating_table(inputs)
#     assert np.allclose(out, expected_outputs, equal_nan = True)


# # Wildcards
# # ---------------------------------------------------------------------
# @pytest.mark.parametrize(
#     'inputs, expected_outputs',
#     [
#         ([[100]], np.array([[3.0]])),
#         ([[19], [100]], np.array([[1.9], [3.0]]))
#     ]
#     )
# def test_rating_table_numeric_wildcards_call(rating_table_simple, inputs, expected_outputs):
#     rating_table = RatingTable(
#         'test_table', rating_table_simple['rating_table_numeric_wildcard'])
#     out = rating_table(inputs)
#     assert np.allclose(out, expected_outputs, equal_nan = True)

# @pytest.mark.parametrize(
#     'inputs, expected_outputs',
#     [
#         ([[100]], np.array([[2.4]])),
#         ([[28], [100]], np.array([[2.3], [2.4]]))
#     ]
#     )
# def test_rating_table_range_wildcards_call(rating_table_simple, inputs, expected_outputs):
#     rating_table = RatingTable(
#         'test_table', rating_table_simple['rating_table_range_wildcard'])
#     out = rating_table(inputs)
#     assert np.allclose(out, expected_outputs, equal_nan = True)

# @pytest.mark.parametrize(
#     'inputs, expected_outputs',
#     [
#         ([[False]], np.array([[3.0]])),
#         ([[True], [False]], np.array([[1.0], [3.0]]))
#     ]
#     )
# def test_rating_table_boolean_wildcards_call(rating_table_simple, inputs, expected_outputs):
#     rating_table = RatingTable(
#         'test_table', rating_table_simple['rating_table_boolean_wildcard'])
#     out = rating_table(inputs)
#     assert np.allclose(out, expected_outputs, equal_nan = True)

@pytest.mark.parametrize(
    'inputs, expected_outputs',
    [
        ([['F1']], np.array([[2.0]])),
        ([['E1'], ['F1'], ['X1']], np.array([[1.8], [2.0], [2.0]]))
    ]
    )
def test_rating_table_str_wildcards_call(rating_table_simple, inputs, expected_outputs):
    rating_table = RatingTable(
        'test_table', rating_table_simple['rating_table_string_wildcard'])
    out = rating_table(inputs)
    assert np.allclose(out, expected_outputs, equal_nan = True)

# @pytest.mark.parametrize(
#     'inputs, expected_outputs',
#     [
#         # ([[70, 'missing', 'X1']], np.array([[100]])),
#         # ([[36, 'missing', 'X1']], np.array([[200]])),
#         ([[20, True, 'A1']], np.array([[1.0]])),
#         ([[20, True, 'X1']], np.array([[100]])),
#         # ([[34, False, 'C1'], [18, True, 'A1']], np.array([[4.8], [1.0]]))
#     ]
#     )
# def test_rating_table_combo_call(rating_table_simple, inputs, expected_outputs):
#     rating_table = RatingTable(
#         'test_table', rating_table_simple['rating_table_combo'])
#     print(rating_table)
#     out = rating_table(inputs)
#     assert np.allclose(out, expected_outputs, equal_nan = True)

# # Have the rating table process a whole dataframe of inputs
# # go straight to testing tables with wildcards
# # =====================================================================
# def test_rating_table_numeric_wildcards_evaluate(rating_table_simple, test_input_simple):
#     print(rating_table_simple['rating_table_numeric_wildcard'])
#     print(rating_table_simple['rating_table_range_wildcard'])
#     rating_table = RatingTable(
#         'test_table', rating_table_simple['rating_table_numeric_wildcard'])
#     out = rating_table.evaluate(test_input_simple)
#     expected_outputs = np.array([[3.0], [1.8], [2.5], [2.1], [3.0]])
#     assert np.array_equal(out.values, expected_outputs)

# def test_rating_table_range_wildcards_evaluate(rating_table_simple, test_input_simple):
#     rating_table = RatingTable(
#         'test_table', rating_table_simple['rating_table_range_wildcard'])
#     out = rating_table.evaluate(test_input_simple)
#     expected_outputs = np.array([[np.nan], [1.8], [2.1], [1.9], [2.4]])
#     assert np.allclose(out.values, expected_outputs, equal_nan = True)

# def test_rating_table_boolean_wildcards_evaluate(rating_table_simple, test_input_simple):
#     rating_table = RatingTable(
#         'test_table', rating_table_simple['rating_table_boolean_wildcard'])
#     out = rating_table.evaluate(test_input_simple)
#     expected_outputs = np.array([[1.0], [1.0], [3.0], [3.0], [3.0]])
#     assert np.allclose(out.values, expected_outputs, equal_nan = True)

# def test_rating_table_str_wildcards_evaluate(rating_table_simple, test_input_simple):
#     rating_table = RatingTable(
#         'test_table', rating_table_simple['rating_table_string_wildcard'])
#     out = rating_table.evaluate(test_input_simple)
#     expected_outputs = np.array([[1.2], [1.4], [1.6], [1.8], [2.0]])
#     assert np.allclose(out.values, expected_outputs, equal_nan = True)