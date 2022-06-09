import pandas as pd

class Info(dict):
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
        super().__init__(**kwargs)
        self.set_proper_index()
        self.check_indices_unique()
    
    def __getattr__(self, name: str):
        if (ret := self.get(name) is not None):
            return ret
        else:
            raise AttributeError(f'Cannot find {name} in info.')
    
    def register(self, **kwargs):
        # updates and checks index relationships
        super().update(kwargs)
        self.ensure_index_joinable()
        self.check_indices_unique()

    def set_proper_index(self):
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

    def check_indices_unique(self):
        for k, v in self.items():
            if isinstance(v, pd.DataFrame):
                if not v.index.is_unique:
                    raise Exception(f'DataFrame {k} has a non-unique index.')

    def get(self, key, default = None, raise_ = False):
        
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
            # Keep track of indices processed for join condition check
            processed_indices = set()
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
                        joined = joined.join(item_to_join)
                    # Add processed indices to set
                    # processed_indices |= current_indices
            
            return joined
    