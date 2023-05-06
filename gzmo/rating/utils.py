import itertools
import pandas as pd
import numpy as np

from gzmo.rating.rating_plan import BaseRatingTable


def make_random_market_basket(rating_plan, market_basket_size, seed = None):
    """Creates a randomized market basket based on inputs from rating tables.

        The function extracts the inputs from all the rating tables in the
            rating plan, and create a market basket with all inputs that are
            needed for the rating plan.
        The market basket produced will be "consistent" in the sense that
            only "sensible" combinations of variables will exist, where
            "sensible" means there will exist a row in a rating table, or
            rows in a collection of rating tables, that allow the input of
            said combination of variables.
        For example, the function will not produce a Toyota Civic, since
            the vehicle symbol table does not have a row for a Toyota Civic.
        This also works across tables. Suppose that in the vehicle symbol
            table, all Honda Crosstours have model year <=2015 (discontinued
            in 2015). Suppose further that in the ADAS feature table,
            only rows for 2020+ exist for forward collision warning system.
            The function will ensure that there are no rows in the market
            basket with a Honda Crosstour that has foward collision warning
            system, even though there are no tables with both the variables
            in the inputs.

    Args:
        rating_plan (gzmo.rating.rating_plan.RatingPlan): A rating plan that
            contains rating tables.
        market_basket_size (int): The size of the market basket.
        seed (int, optional): The seed for np.random.default_rng.
            Defaults to None.

    Returns:
        _type_: _description_
    """
    disjoint_input_sets = []

    rating_steps_to_process = {
        name: step
        for name, step in rating_plan.items()
        if isinstance(step, BaseRatingTable)
    }

    # Get set of all outputs:
    # if these also happen to be inputs, we can ignore them
    outputs = list(itertools.chain(
        *[step.outputs for step in rating_steps_to_process.values()]
        ))

    rating_steps_processed = []

    for name_i, step_i in rating_steps_to_process.items():

        if name_i in rating_steps_processed:
            continue
        
        # drop from index any inputs that also exist as an output (elsewhere)
        also_outputs = list(set(step_i.inputs) & set(outputs))
        step_i = step_i.reset_index(also_outputs, drop = True)
        # drop duplicates
        step_i = step_i.loc[~step_i.index.duplicated()]

        # if this table doesn't have inputs, no work to do
        if not step_i.inputs:
            continue
        
        # define the numeric and non-numeric inputs
        step_i_numeric_inputs = [
            i for i in step_i.inputs
            if isinstance(step_i.index.get_level_values(i), pd.IntervalIndex) \
                or step_i.index.is_numeric()

            ]
        step_i_non_numeric_inputs = \
            list(set(step_i.inputs) - set(step_i_numeric_inputs))

        # get all associated rating steps
        # i.e. has any common non-numeric, non-outupt inputs with step_i
        associated_rating_steps = {
            name_j: step_j.loc[:, []]
            for name_j, step_j in rating_plan.items()
            if (
                (
                    set(step_j.inputs)
                    & (set(step_i_non_numeric_inputs) - set(outputs))
                )
                and (name_j != name_i)
            )
        }

        # process each associated step and join by index if appropriate
        joined = step_i.copy()
        limit = 1000000
        for name_j, step_j in associated_rating_steps.items():

            # first remove any inputs that is either:
            #   - numeric and exists in step_i.inputs
            #   - an output elsewhere
            # By definition of `associated_rating_steps`,
            #   there should be other inputs left.
            to_remove = list(
                set(step_j.inputs) &
                (set(step_i_numeric_inputs) | set(outputs))
                )
            step_j = step_j.reset_index(to_remove, drop = True)
            # drop duplicates
            step_j = step_j.loc[~step_j.index.duplicated()]

            # limit the number of rows to sample from
            joined = joined.sample(min(len(joined), int(limit / len(step_j))))
            joined = \
                joined.merge(
                    step_j,
                    left_index = True,
                    right_index = True,
                    how = 'left'
                    )
            
            rating_steps_processed.append(name_j)

        joined = joined.loc[:, []].reset_index(drop = False)
        # drop wildcards
        joined = joined.loc[~(joined == '*').any(axis = 1)]

        # add to the set to later sample from
        disjoint_input_sets.append(joined)

    # Finished creating dataset to sample from
    # Now begin sampling
    sample = None
    rng = np.random.default_rng(seed = seed)
    for input_set_i in disjoint_input_sets:
        sampled = \
            input_set_i \
            .sample(market_basket_size, random_state = rng, replace = True) \
            .reset_index(drop = True)
        
        # sample from interval columns
        for c in sampled.columns:
            if sampled[c].dtype == 'interval':
                left = sampled[c].map(lambda i: i.left).clip(-9999, 9999)
                right = sampled[c].map(lambda i: i.right).clip(-9999, 9999)
                is_integer = \
                    (
                        ((left % 1) == 0) & \
                        ((right % 1) == 0)
                    ).all()

                if is_integer:
                    # if all whole numbers, generate an integer
                    sampled[c] = rng.integers(
                        left,
                        right,
                        size = len(sampled[c]),
                        endpoint = True
                        )
                else:
                    # otherwise generate a floating point number
                    sampled[c] = rng.uniform(
                        left,
                        right,
                        size = len(sampled[c])
                        )
        
        # add it to the sampled dataset
        if sample is None:
            sample = sampled
        else:
            sample = \
                pd.concat([sample, sampled], axis = 1)
        
    # finally add a unique index
    c = sample.columns
    sample['idx'] = sample.index
    sample = sample.loc[:, ['idx'] + list(c)]

    # output dataset
    return sample


def make_market_basket(dict_specification, market_basket_size, seed = None):
    """Create a market basket based on the specification provided.

    Args:
        dict_specification (dict): A dictionary specifying what each
            attribute should look like in the market basket. See
            examples below.
        market_basket_size (int): The size of the market basket.
        seed (int, optional): The seed for np.random.default_rng.
            Defaults to None.

    Returns:
        pd.DataFrame: The simulated market basket.
    
    Examples of dictionary entries and resulting dataframe columns:
        - `SERIAL`: similar to SQL's keyword, the column autoincrements
            and assigns an id to each row. Starts at 1.
        - `(a, b)`: a tuple indicates a uniform(a, b) variable. When both
            `a` and `b` are integers, the returned values are integers.
            Otherwise, a float is returned.
        - Objects of lenth 1 or objects with no lenth attribute: The
            resulting market basket column will take this value for each row.
            For example:
                - `X`
                - `['Internet']`
                - `3`
                - `20.0`
        - Objects of lenth `n > 1`: `np.random.choice` will be used to
            randomly select a value from the iterable for each row.
            For example:
                - `'IRX'`
                - `['I', 'R', 'X']`
                - `[1, 2, 3]`
        - Object of lenth `market_basket_size`: This allows the user to
            directly pass a column of the market basket, for custom
            actions not otherwise considered in this constructor.
            For example:
                - `np.rint(np.random.normal(30, 20, market_basket_size))`
                - `pd.Series` of lenth `market_basket_size`

    """    
    processed_dict = dict()
    rng = np.random.default_rng(seed = seed)

    for k, v in dict_specification.items():
        if str(v) == 'SERIAL':
            processed_dict[k] = np.arange(market_basket_size) + 1
        elif (isinstance(v, tuple)) and (len(v) == 2):
            if (isinstance(v[0], int) and isinstance(v[1], int)):
                processed_dict[k] = \
                    rng.integers(
                        v[0], v[1], market_basket_size,
                        endpoint = True
                        )
            else:
                processed_dict[k] = \
                    v[0] + (v[1] - v[0]) * rng.random(market_basket_size)
        elif hasattr(v, '__len__'):
            if len(v) == 1:
                arr = np.empty(market_basket_size, dtype = object)
                arr[:] = v
                processed_dict[k] = arr
            elif len(v) == market_basket_size:
                processed_dict[k] = np.array(v) # to reindex
            else:
                # Is it possible for an objet to
                #   have __len__ yet not be iterable?
                processed_dict[k] = \
                    rng.choice(list(v), market_basket_size)
        else:
            arr = np.empty(market_basket_size, dtype = object)
            arr[:] = v
            processed_dict[k] = arr 

    df = pd.DataFrame(processed_dict)

    return df
