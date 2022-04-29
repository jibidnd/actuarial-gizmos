import pandas as pd
import numpy as np
import pytest

from gzmo.rating.rating_plan import RatingTable

# Fixtures
# =====================================================================
# no wildcards
@pytest.fixture
def rating_table_numeric():
    df = pd.DataFrame({
        '_age': [18, 19, 20, 21, 22, 23, 24, 25],
        'factor_': [1.8, 1.9, 2.0, 2.1, 2.2, 2.3, 2.4, 2.5]
    })
    return df

@pytest.fixture
def rating_table_range():
    df = pd.DataFrame({
        '_age_left': [18, 23, 26, 30, 36, 41, 43],
        '_age_right': [22, 25, 29, 35, 40, 42, 65],
        'factor_': [1.8, 1.9, 2.0, 2.1, 2.2, 2.3, 2.4]
    })
    return df

@pytest.fixture
def rating_table_boolean():
    df = pd.DataFrame({
        '_safe_driving': [True, False],
        'factor_': [1.0, 1.5]
    })
    return df

@pytest.fixture
def rating_table_str():
    df = pd.DataFrame({
        '_credit_tier': ['A1', 'B1', 'C1', 'D1', 'E1'],
        'factor_': [1.0, 1.2, 1.4, 1.6, 1.8]
    })
    return df

# same thing but with wildcards
@pytest.fixture
def rating_table_numeric_wildcard():
    df = pd.DataFrame({
        '_age': [18, 19, 20, 21, 22, 23, 24, 25, '*'],
        'factor_': [1.8, 1.9, 2.0, 2.1, 2.2, 2.3, 2.4, 2.5, 3.0]
    })
    return df

@pytest.fixture
def rating_table_range_wildcard():
    df = pd.DataFrame({
        '_age_left': [18, 20, 22, 24, 26, 28, 30],
        '_age_right': [19, 21, 23, 25, 27, 29, '*'],
        'factor_': [1.8, 1.9, 2.0, 2.1, 2.2, 2.3, 2.4]
    })
    return df

@pytest.fixture
def rating_table_boolean_wildcard():
    df = pd.DataFrame({
        '_safe_driving': [True, '*'],
        'factor_': [1.0, 3.0]
    })
    return df

@pytest.fixture
def rating_table_str_wildcard():
    df = pd.DataFrame({
        '_credit_tier': ['A1', 'B1', 'C1', 'D1', 'E1', '*'],
        'factor_': [1.0, 1.2, 1.4, 1.6, 1.8, 2.0]
    })
    return df

# Tests
# =====================================================================
# Simple call of the rating table with 1 or two sets of inputs
# ---------------------------------------------------------------------
# No wildcards
# ---------------------------------------------------------------------
@pytest.mark.parametrize(
    'inputs, outputs',
    [
        ([[18]], np.array([[1.8]])),
        ([[19], [20]], np.array([[1.9], [2.0]]))
    ]
    )
def test_rating_table_numeric_call(rating_table_numeric, inputs, outputs):
    rating_table = RatingTable('test_table', rating_table_numeric)
    out = rating_table(inputs)
    assert np.array_equal(out.values, outputs)

@pytest.mark.parametrize(
    'inputs, outputs',
    [
        ([[18]], np.array([[1.8]])),
        ([[27], [60]], np.array([[2.0], [2.4]]))
    ]
    )
def test_rating_table_range_call(rating_table_range, inputs, outputs):
    rating_table = RatingTable('test_table', rating_table_range)
    out = rating_table(inputs)
    assert np.array_equal(out.values, outputs)

@pytest.mark.parametrize(
    'inputs, outputs',
    [
        ([[True]], np.array([[1.0]])),
        ([[True], [False]], np.array([[1.0], [1.5]]))
    ]
    )
def test_rating_table_boolean_call(rating_table_boolean, inputs, outputs):
    rating_table = RatingTable('test_table', rating_table_boolean)
    out = rating_table(inputs)
    assert np.array_equal(out.values, outputs)

@pytest.mark.parametrize(
    'inputs, outputs',
    [
        ([['A1']], np.array([[1.0]])),
        ([['A1'], ['E1']], np.array([[1.0], [1.8]]))
    ]
    )
def test_rating_table_str_call(rating_table_str, inputs, outputs):
    rating_table = RatingTable('test_table', rating_table_str)
    out = rating_table(inputs)
    assert np.array_equal(out.values, outputs)


# Wildcards
# ---------------------------------------------------------------------
@pytest.mark.parametrize(
    'inputs, outputs',
    [
        ([[100]], np.array([[3.0]])),
        ([[19], [100]], np.array([[1.9], [3.0]]))
    ]
    )
def test_rating_table_numeric_wildcards_call(rating_table_numeric_wildcard, inputs, outputs):
    rating_table = RatingTable('test_table', rating_table_numeric_wildcard)
    out = rating_table(inputs)
    assert np.array_equal(out.values, outputs)

@pytest.mark.parametrize(
    'inputs, outputs',
    [
        ([[100]], np.array([[2.4]])),
        ([[28], [100]], np.array([[2.3], [2.4]]))
    ]
    )
def test_rating_table_range_wildcards_call(rating_table_range_wildcard, inputs, outputs):
    rating_table = RatingTable('test_table', rating_table_range_wildcard)
    out = rating_table(inputs)
    assert np.array_equal(out.values, outputs)

@pytest.mark.parametrize(
    'inputs, outputs',
    [
        ([[False]], np.array([[3.0]])),
        ([[True], [False]], np.array([[1.0], [3.0]]))
    ]
    )
def test_rating_table_boolean_wildcards_call(rating_table_boolean_wildcard, inputs, outputs):
    rating_table = RatingTable('test_table', rating_table_boolean_wildcard)
    out = rating_table(inputs)
    assert np.array_equal(out.values, outputs)

@pytest.mark.parametrize(
    'inputs, outputs',
    [
        ([['F1']], np.array([[2.0]])),
        ([['E1'], ['F1']], np.array([[1.8], [2.0]]))
    ]
    )
def test_rating_table_str_wildcards_call(rating_table_str_wildcard, inputs, outputs):
    rating_table = RatingTable('test_table', rating_table_str_wildcard)
    out = rating_table(inputs)
    assert np.array_equal(out.values, outputs)