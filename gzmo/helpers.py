import itertools

import pandas as pd

def set_unique_index(df: pd.DataFrame, max_cols):
    if (df.index.is_unique) & \
        (df.index.names != [None]):
        return df

    for num_cols in range(1, max_cols + 1):
        for addl_idx_size in range(1, num_cols + 1):
            lst_cols = df.columns[:num_cols]
            if len(lst_cols) == 0:
                raise Exception('Too few columns to find unique index.')
            sample_idxs = \
                itertools.combinations(lst_cols, addl_idx_size)
            for sample_idx in sample_idxs:
                lvls_to_ignore = \
                    [
                        i for
                        i, name in enumerate(df.index.names)
                        if name is None
                    ]
                test_idx = \
                    df.set_index(list(sample_idx), append = True) \
                        .reset_index(level = lvls_to_ignore) \
                        .index
                if test_idx.is_unique:
                    df = df.drop(list(sample_idx), axis = 1)
                    df = df.set_index(test_idx)
                    return df
    msg = f'Unable to find unique index among columns {",".join(lst_cols)}'
    raise Exception(msg)