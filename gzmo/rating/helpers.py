import pandas as pd
import numpy as np

def process_rating_table(df, wildcard_characters = None) -> pd.DataFrame:
    """Parse the rating table.

    Rules:
        1. All inputs start wtih "_"
        2. Interval inputs start with "_" and have two columns ending
                with "_left" and "_right"
        3. All outputs end with "_"

    This creates a pd.MultiIndex for the dataframe. A MultiIndex
        allows using multiple inputs. To keep things consistent,
        we will use a MultiIndex regardless of how many inputs
        there are. One can easily convert it back to a single index.
    """


    # use the default set of wildcard characters if none is provided.
    wildcard_characters = \
        wildcard_characters or \
        ['*', pd.Interval(-np.inf, np.inf, closed = 'both')]

    # keep track of inputs and outputs.
    interval_inputs = []
    other_inputs = []
    inputs = []
    outputs = []

    # First loop through columns to identify inputs and outputs
    for c in df.columns:
        if c.startswith('_'):
            # Ranges
            # note that a single wildcard in one but not both ends of a range
            # are not considered true "wildcard" uses.
            # see treatment of double_wildcard and single_wildcard
            # below for more information
            if c.endswith('_left'):
                cleaned = c[1:].replace('_left', '').replace('_right', '')
                # Add to list of inputs
                interval_inputs.append(cleaned)
                inputs.append(cleaned)
                # Convert wildcards to -inf and
                # cast to float for performance
                df[c] = df[c].replace(wildcard_characters, -np.inf).astype(float)
            elif c.endswith('_right'):
                # _right columns don't need to be added to input list
                df[c] = df[c].replace(wildcard_characters, np.inf).astype(float)
            # Other inputs
            else:
                # add to input list
                cleaned = c[1:]
                other_inputs.append(cleaned)
                inputs.append(cleaned)
                # # Make a note of wildcard usage, if any
                # if (df[c]=='*').any():
                #     _has_wildcards = True
        elif c.endswith('_'):
            # add outputs to the list of outputs
            cleaned = c[:-1]
            outputs.append(cleaned)
    
    # Create new index
    # Note that interval ranges are assumed to be closed on both ends,
    #  consistent with how rating tables are usually defined. 
    new_indices = []
    for c in inputs:
        if c in interval_inputs:
            # Check that if interval, both _left and _right are defined
            assert ((f'_{c}_left' in df.columns)
                    and (f'_{c}_right' in df.columns)), \
                f'Missing column for interval input {c}'
            # create pandas interval index
            idx = pd.IntervalIndex.from_arrays(
                df[f'_{c}_left'], df[f'_{c}_right'],
                closed = 'both',
                name=c
            )
            new_indices.append(idx)
        else:
            idx = pd.Index(df[f'_{c}'], name = c)
            new_indices.append(idx)
    
    # set multiindex
    if len(new_indices) > 0:
        # create new dataframe
        # Cannot use df.set_index here as that apparently
        #   flattens the multiindex if it only has one level.
        #   See source code for details.
        #   I don't understand it that well.
        df.index = pd.MultiIndex.from_arrays(new_indices)
    
    # subset on output columns only
    # if len(outputs) > 0:
    # rename the columns
    df = df.rename(columns = {f'{c}_': c for c in outputs})
    df = df.loc[:, outputs]

    return df, inputs, outputs

def get_wildcard_markers(input_columns, wildcard_characters):
    return input_columns.isin(wildcard_characters).any(axis = 1)