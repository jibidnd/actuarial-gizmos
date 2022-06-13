import itertools
import copy
import pandas as pd

class Book(dict):
    """A dictionary-like class to allow access to multiple objects' properties

    While there is functionality to automatically set the indices,
    it works by identifying columns that appear in multiple dataframes.
    For dataframes whose uniquely identifying rows do not appear in other
    dataframes, the index will not be properly set.

    For example, a violations dataframe may have the columns:
        license_number | violation_date | violation_code
    While license_number likely appears in another dataframe and will be
    automatically selected as an index, violation_date will not. This may
    cause the dataframe to have a non-unique index if the user does not
    manually index the dataframe with uniquely identifying rows.
    """    
    def __init__(self, **kwargs):
        super().__init__(copy.deepcopy(kwargs))
        self.set_joinable_indices()
        self.set_unique_indices()
    
    def __getattr__(self, name: str):
        if (ret := self.get(name) is not None):
            return ret
        else:
            raise AttributeError(f'Cannot find {name} in info.')
    
    def register(self, **kwargs):
        # updates and checks index relationships
        super().update(kwargs)
        self.set_joinable_index()
        self.check_indices_unique()

    def set_joinable_indices(self):
        # this is to check that if a column is used as an index
        # in any dataframe, it is used as an index everywhere else.
        # This is such that joining can be done efficiently.

        # first get all the names
        all_names = dict()
        for v in self.values():
            if isinstance(v, pd.DataFrame):
                all_names.update({idx: 0 for idx in v.index.names})
                all_names.update({c: 0 for c in v.columns})
        
        # then count how many dataframes they appear in
        for name in all_names:
            for v in self.values():
                if isinstance(v, pd.DataFrame):
                    df_names = set(v.index.names) | set(v.columns)
                    if name in df_names:
                        all_names[name] += 1
        
        # for anything that exist in >1 dataframes,
        # set it an index for all dataframes
        lst_idx = [name for name, count in all_names.items() if count > 1]
        for v in self.values():
            if isinstance(v, pd.DataFrame):
                add_to_idx = [c for c in v.columns if c in lst_idx]
                v.set_index(add_to_idx, append = True, inplace = True)
                # drop an index if it doesn't have a name
                lvls_to_drop = \
                    [i for i, name in enumerate(v.index.names) if name is None]
                v.reset_index(level = lvls_to_drop, drop = True, inplace = True)

    def set_unique_indices(self, max_cols = 5):
        # for each data frame
        #   if dataframe has non-unique index
        #       test all combinations up to `max_cols`
        #       to see if we can create a unique index
        for k, v in self.items():
            if isinstance(v, pd.DataFrame):
                if not v.index.is_unique:
                    try:
                        self[k] = set_unique_index(v, max_cols)
                    except Exception as err:
                        msg = f'Unable to set unique index for dataframe {k}'
                        raise Exception(msg) from err

    def get(self, key, default = None, how = 'left', raise_ = True):
        
        # searching for a single key should land here
        try:
            # First see if there is an info matching the name
            if (ret := super().get(key)) is not None:
                return ret
            # Then try to loop through each info to see if we
            # get anything directly
            for k, v in self.items():
                # allow index to be searchable too
                if isinstance(v, pd.DataFrame):
                    idx_columns = v.index.to_frame()
                    keep = idx_columns.columns.difference(v.columns)
                    idx_columns = idx_columns[keep]
                    to_search = pd.concat([v, idx_columns], axis = 1)
                else:
                    to_search = v

                if (ret := to_search.get(key)) is not None:
                    return ret
        except TypeError:
            pass
        
        # if the key is a single string,
        # getting here means we can't find it
        if isinstance(key, str):
            if raise_:
                raise AttributeError(f'Attribute {key} not found.')
            else:
                return default
        
        # Otherwise, treat the key as an iterable
        # If we get here, the key / collection of keys
        #   does not belong to any item in self.
        # Attempt to do some joinery (only works when each item
        #  requested in `key` is a pd.Series with joinable indices).
        joined = None
        try:
            # remove duplicates if any
            unique_keys = list(dict.fromkeys(key))
        except TypeError as e:
            # If the key is not iterable and not a string.
            # it is a single key that cannot be found.
            if raise_:
                raise AttributeError(f'Attribute {key} not found.')
            else:
                return default
        else:
            # Keep track to preserve order of indices
            # for some reason `pd.join` sometimes rearranges the indices
            processed_indices = []
            for k in unique_keys:
                if (item_to_join := self.get(k)) is None:
                    # Cannot get anything for this key.
                    if raise_:
                        raise AttributeError(f'Attribute {k} not found.')
                    else:
                        return default
                else:
                    if joined is None:
                        joined = item_to_join.to_frame()
                    else:
                        joined = joined.join(item_to_join, how = how)
                    new_indices = [
                        idx for idx in joined.index.names
                        if idx not in processed_indices
                        ]
                    processed_indices += new_indices
            
            joined = joined.reorder_levels(processed_indices)
            
            return joined

def set_unique_index(df, max_cols):
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
                    df.set_index(test_idx, inplace = True)
                    return df
    msg = f'Unable to find unique index among columns {",".join(lst_cols)}'
    raise Exception(msg)