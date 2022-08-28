import copy

import pandas as pd

from gzmo.helpers import set_unique_index

class DotDict(dict):
    
    def __getattr__(self, key):
        if (ret := self.get(key)) is not None:
            return ret
        else:
            # Raise an error
            raise AttributeError(f'Attribute {key} does not exist.')
    
    def __setattr__(self, key, value):
        self[key] = value
        return

class FancyDict(DotDict):
    """A dictionary-like class that allow attribute access.
        Adding a class to allow a unified API.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def get(self, key: str, default = None):
        # `get` will allow a recursive search.

        # First check if any key is equal to `key`
        try:
            if (res := super().get(key)) is not None:
                return res
        except TypeError:
            # e.g. a list is passed. The top level dict
            # may not take a list as a key but maybe a lower
            # level item could be a SearchableDict.
            pass

        # Otherwise loop through each item
        # to see if we can `get` the requested key
        for val in self.values():
            try:
                if (ret := val.get(key)) is not None:
                    return ret
            except (AttributeError, TypeError):
                pass
        
        # If we get here that means we didn't find anything
        # Return the default value
        return default

    def register(self, **kwargs) -> None:
        # Override to add checks
        super().update(kwargs)

class SearchableDict(FancyDict):
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
        super().__init__(**copy.deepcopy(kwargs))
        self.set_unique_indices()
        self.set_joinable_indices()
    
    # def __getattr__(self, name: str):
    #     if (ret := self.get(name)) is not None:
    #         return ret
    #     else:
    #         raise AttributeError(f'Cannot find {name} in info.')
    
    def __getitem__(self, key: any):
        if (ret := self.get(key)) is not None:
            return ret
        else:
            raise AttributeError(f'Cannot find {key} in info.')

    def register(self, **kwargs):
        # updates and checks index relationships
        super().update(kwargs)
        self.set_joinable_indices()
        self.set_unique_indices()

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
                add_to_idx = [
                    c for c in v.columns
                    if c in lst_idx
                    if c not in v.index.names
                    ]
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
                if not ((v.index.is_unique) & (v.index.names != [None])):
                    try:
                        self[k] = set_unique_index(v, max_cols)
                    except Exception as err:
                        msg = f'Unable to set unique index for dataframe {k}'
                        raise Exception(msg) from err
                    except:
                        raise

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

            # If nothing is requested, simply return the indices
            # of the first dataframe
            if len(unique_keys) == 0:
                return next(iter(self.values())).get([])

            for k in unique_keys:
                if (item_to_join := self.get(k)) is None:
                    # Cannot get anything for this key.
                    if raise_:
                        raise AttributeError(f'Attribute {k} not found.')
                    else:
                        return default
                else:
                    if joined is None:
                        # TODO: allow non-dataframe objects (e.g. defaults, arrays)
                        joined = item_to_join.to_frame()
                    else:
                        joined = joined.join(item_to_join, how = how)
                    new_indices = [
                        idx for idx in joined.index.names
                        if idx not in processed_indices
                        ]
                    processed_indices += new_indices
            # reorder if there are more than 1 column returned
            if len(processed_indices) > 1:
                joined = joined.reorder_levels(processed_indices)
            
            return joined

class AccessLogger:
    """Logs the attributes accessed of the object.
    Any attribute accessed will be stored in the form of
        a tuple, with each element containing the complete
        path of access from the root object.
    """    
    
    def __init__(self, accessed = None, root = None, recursive = False):
        self.accessed = accessed or set()
        self.root = root or tuple()
        self.recursive = recursive
    
    def __getattr__(self, key: str):
        return self.get(key)
    
    def __getitem__(self, key: str):
        return self.get(key)

    def __call__(self, *args, **kwargs):
        # path_to_current = self.root + (*args,)
        # # add full path to "accessed" list
        # self.accessed |= {path_to_current}
        # print(self.accessed)
        # # return an AccessLogger object with reference to the original list
        # if self.recursive:
        #     return AccessLogger(self.accessed, path_to_current)
        # else:
        #     return AccessLogger()
        return self
    
    # overload the operators user can add/subtract/multiply/etc the logger
    def __add__(self, other):
        return self
    
    def __sub__(self, other):
        return self
    
    def __mul__(self, other):
        return self
    
    def __pow__(self, other):
        return self
    
    def __truediv__(self, other):
        return self
    
    def __floordiv__(self, other):
        return self
    
    def __mod__(self, other):
        return self
    
    def __lshift__(self, other):
        return self
    
    def __rshift__(self, other):
        return self
    
    def __and__(self, other):
        return self
    
    def __or__(self, other):
        return self
    
    def __xor__(self, other):
        return self
    
    def __invert__(self):
        print(self)
        return self

    def get(self, key: list or str = None):
        # complete path to current item
        if isinstance(key, list):
            paths_to_current = [self.root + (key_i, ) for key_i in key]
            path_to_current = self.root + (str(key), )
            # add full paths to "accessed" list
            self.accessed |= {*paths_to_current}
        else:
            # If this is a pandas dataframe operation, don't log it
            if hasattr(pd.DataFrame, key) or hasattr(pd.Series, key):
                return self
            path_to_current = self.root + (key,)
            # add full path to "accessed" list
            self.accessed |= {path_to_current}
            # return an AccessLogger object with reference to the original list
        if self.recursive:
            return AccessLogger(self.accessed, path_to_current)
        else:
            return AccessLogger()
