import copy
import functools

import pandas as pd

from gzmo.helpers import set_unique_index
class DotDict(dict):
    """A dictionary that allows "dot" attribute access.

    Args:
        dict
    """    
    def __getattr__(self, key):
        if (ret := self.get(key)) is not None:
            return ret
        else:
            # Raise an error
            raise AttributeError(f'Attribute {key} does not exist.')
    
    # def __setattr__(self, key, value):
    #     self[key] = value
    #     return

class FancyDict(DotDict):
    """A dictionary-like class that allow accessing attributes in child items.s
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
    def __init__(self, joinable_indices = True, **kwargs):
        super().__init__(**copy.deepcopy(kwargs))
        self.set_unique_indices()
        self.joinable_indices = joinable_indices
        if self.joinable_indices:
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
        """Overrides parent method to updates and checks index relationships.
        """        
        super().update(kwargs)
        self.set_unique_indices()
        if self.joinable_indices:
            self.set_joinable_indices()

    def set_joinable_indices(self):
        """Make sure that all dataframes are easily joinable.

            This is to check that if a column is used as an index
            in any dataframe, it is used as an index everywhere else.
            This is such that joining can be done efficiently.
        """        

        # first get all the names
        all_names = dict()
        dataframes = [v for v in self.values() if isinstance(v, pd.DataFrame)]
        for v in dataframes:
            all_names.update({idx: 0 for idx in v.index.names})
            all_names.update({c: 0 for c in v.columns})
        
        # then count how many dataframes they appear in
        for name in all_names:
            for v in dataframes:
                df_names = set(v.index.names) | set(v.columns)
                if name in df_names:
                    all_names[name] += 1
        
        # for anything that exist in >1 dataframes,
        # set it an index for all dataframes
        lst_idx = [name for name, count in all_names.items() if count > 1]
        for v in dataframes:
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
        """This allows easy access of attribute(s).

        Args:
            key (string or list): string or list of attributes to get.
            default (optional): The default value to return of
                no results are found. Defaults to None.
            how (str, optional): How dataframes are joined if multiple
                attributes requested are on different levels. Defaults to 'left'.
            raise_ (bool, optional): Whether errors are raised. Defaults to True.

        Raises:
            AttributeError
            AttributeError
            AttributeError
            NotImplementedError
            NotImplementedError

        Returns:
            The requested attribute(s).
        """        
        # searching for a single key should land here
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
            try:
                if (ret := to_search.get(key)) is not None:
                    return ret
            except (TypeError, AttributeError):
                pass
            except:
                raise
        
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
                for v in self.values():
                    try:
                        return v.get([])
                    except StopIteration:
                        pass
                return

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
                        if isinstance(item_to_join, pd.Series):
                            joined = item_to_join.to_frame(k)
                        elif isinstance(item_to_join, pd.DataFrame):
                            joined = item_to_join
                        else:
                            raise NotImplementedError
                    else:
                        if isinstance(item_to_join, pd.Series):
                            joined = joined.join(item_to_join.rename(k), how = how)
                        elif isinstance(item_to_join, pd.DataFrame):
                            joined = joined.join(item_to_join, how = how)
                        else:
                            raise NotImplementedError
                            
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
    
    def __init__(self, accessed = None, root = None, depth = 1):
        self.accessed = accessed or set()
        self.root = root or tuple()
        self.depth = depth
        self.current_depth = 0
    
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
    def __eq__(self, other):
        return True
    
    def __ge__(self, other):
        return True
    
    def __gt__(self, other):
        return True
    
    def __le__(self, other):
        return False
    
    def __lt__(self, other):
        return False

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
        return self
    
    def __iter__(self):
        return self
    
    def __next__(self):
        return self


    def get(self, key: list or str = None):
        # Only log an access if the current depth is < self.depth
        if len(self.root) < self.depth:
            if isinstance(key, AccessLogger):
                return self
            elif isinstance(key, list):
                paths_to_current = [self.root + (key_i, ) for key_i in key]
                # complete path to current item
                path_to_current = self.root + (str(key), )
                # add full paths to "accessed" list
                self.accessed |= {*paths_to_current}
            else:
                # If this is a pandas dataframe operation, don't log it
                if hasattr(pd.DataFrame, key) or hasattr(pd.Series, key):
                    return self
                # complete path to current item
                path_to_current = self.root + (key,)
                # add full path to "accessed" list
                self.accessed |= {path_to_current}
                # return an AccessLogger object with reference to the original list
            
            # return an access logger for any further attributes
            return AccessLogger(self.accessed, path_to_current)

        # Still have to faciliate the rest of the function
        #   even if past the sepcified depth though, to let the
        #   function finish smoothly.
        else:
            return self

class FancyDF(pd.DataFrame):
    """To override some of panda's operations.
    """

    # to retain subclasses through pandas data manipulations
    # https://pandas.pydata.org/docs/development/extending.html
    # Also see https://github.com/pandas-dev/pandas/issues/19300
    @property
    def _constructor(self):
        return FancyDF

    @property
    def _constructor_sliced(self):
        return FancySeries
    
    def maintain_column_order(func):
        @functools.wraps(func)
        def decorated(self, other, *args, **kwargs):
            if isinstance(other, pd.DataFrame):
                cols = [*self.columns, *other.columns.difference(self.columns)]
                return func(self, other, *args, **kwargs).loc[:, cols]
            else:
                return func(self, other, *args, **kwargs)
        return decorated
    
    def align_index(func):
        @functools.wraps(func)
        def decorated(self, other, *args, **kwargs):
            # pandas is not smart enough to align series
            #   with dataframe columns
            # See https://stackoverflow.com/questions/35714582/notimplementederror-fill-value-0-not-supported
            if isinstance(other, pd.Series):
                aligned, _ = other.align(self, axis = 0)
                other_df = FancyDF(
                    {k: aligned.values for k in self.columns},
                    index = self.index
                )
                return func(self, other_df, *args, axis = 0, **kwargs)
            else:
                return func(self, other, *args, **kwargs)
        return decorated


    # override pd.DataFrame's operations
    @align_index
    @maintain_column_order
    def add(self, other, *args, **kwargs):
        kwargs.update(fill_value = 0)
        return super().add(other, *args, **kwargs)

    def __add__(self, other):
        return self.add(other)

    radd = add
    __radd__ = __add__

    @align_index
    @maintain_column_order
    def sub(self, other, *args, **kwargs):
        kwargs.update(fill_value = 0)
        return super().sub(other, *args, **kwargs)

    def __sub__(self, other):
        return self.sub(other)

    rsub = sub
    __rsub__ = __sub__

    @align_index
    @maintain_column_order
    def mul(self, other, *args, **kwargs):
        kwargs.update(fill_value = 1)
        return super().mul(other, *args, **kwargs)

    def __mul__(self, other):
        return self.mul(other)

    rmul = mul
    __rmul__ = __mul__

    @align_index
    @maintain_column_order
    def truediv(self, other, *args, **kwargs):
        kwargs.update(fill_value = 1)
        return super().div(other, *args, **kwargs)
    
    def __truediv__(self, other):
        return self.div(other)

    div = truediv
    rdiv = truediv
    __div__ = __truediv__
    __rdiv__ = __truediv__
    
    truediv = div
    
    @align_index
    @maintain_column_order
    def mod(self, other, *args, **kwargs):
        kwargs.update(fill_value = 1)
        return super().div(other, *args, **kwargs)

    def __mod__(self, other):
        return self.mod(other)
    
    @align_index
    @maintain_column_order
    def pow(self, other, *args, **kwargs):
        kwargs.update(fill_value = 1)
        return super().pow(other, *args, **kwargs)

    def __pow__(self, other):
        return self.pow(other)

class FancySeries(pd.Series):
    """To override some of panda's operations.
    """
        
    # to retain subclasses through pandas data manipulations
    # https://pandas.pydata.org/docs/development/extending.html
    # Also see https://github.com/pandas-dev/pandas/issues/19300
    @property
    def _constructor(self):
        return FancySeries
    
    @property
    def _constructor_expanddim(self):
        return FancyDF    

    # override pd.Series's operations
    def add(self, other, *args, **kwargs):
        kwargs.update(fill_value = 0)
        return super().add(other, *args, **kwargs)

    def __add__(self, other):
        if isinstance(other, pd.Series):
            return self.add(other)
        else:
            return super().__add__(other)

    radd = add
    __radd__ = __add__

    def sub(self, *args, **kwargs):
        kwargs.update(fill_value = 0)
        return super().sub(*args, **kwargs)

    def __sub__(self, other):
        if isinstance(other, pd.Series):
            return self.sub(other)
        else:
            return super().__sub__(other)

    rsub = sub
    __rsub__ = __sub__

    def mul(self, other, *args, **kwargs):
        kwargs.update(fill_value = 1)
        return super().mul(other, *args, **kwargs)

    def __mul__(self, other):
        if isinstance(other, pd.Series):
            return self.mul(other)
        else:
            return super().__mul__(other)

    rmul = mul
    __rmul__ = __mul__

    def truediv(self, *args, **kwargs):
        kwargs.update(fill_value = 1)
        return super().div(*args, **kwargs)

    def __truediv__(self, other):
        if isinstance(other, pd.Series):
            return self.div(other)
        else:
            return super().__div__(other)

    div = truediv
    rdiv = truediv
    __div__ = __truediv__
    __rdiv__ = __truediv__

    truediv = div 
    
    def mod(self, *args, **kwargs):
        kwargs.update(fill_value = 1)
        return super().div(*args, **kwargs)

    def __mod__(self, other):
        if isinstance(other, pd.Series):
            return self.mod(other)
        else:
            return super().__mod__(other)
    
    def pow(self, *args, **kwargs):
        kwargs.update(fill_value = 1)
        return super().pow(*args, **kwargs)

    def __pow__(self, other):
        if isinstance(other, pd.Series):
            return self.pow(other)
        else:
            return super().__pow__(other)