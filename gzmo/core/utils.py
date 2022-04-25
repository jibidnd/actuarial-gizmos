import itertools

class Info:
    """A convenient class to allow access to multiple objects' properties
    """    
    def __init__(self, *args):
        self._infos = []
        self._infos.extend(args)
    
    def __getattr__(self, name: str):
        if (ret := self.get(name) is not None):
            return ret
        else:
            raise AttributeError(f'Cannot find {name} in info.')
    
    def register(self, info):
        self._infos.append(info)

    def get(self, key, default = None):
        
        # First try to loop through each info to see if we
        # get anything directly
        for info in self._infos:
            if ret := info.get(key) is not None:
                return ret
        
        # if the key is a single string,
        # getting here means we can't find it
        if isinstance(key, str):
            return default
        
        # If we get here, the key does not belong to a
        #   single object in self._infos.
        # Attempt to do some joinery (only works when each item
        #  requested in `key` is a pd.Series with joinable indices).
        joined = None
        try:
            iterator = iter(key)
        except TypeError as e:
            # If the key is not iterable and not a string.
            # it is a single key that cannot be found.
            return default
        else:
            # Flatten the iterator to treat each item sequentially
            flattened_keys = list(itertools.chain.from_iterable(iterator))
            # Keep track of indices processed for join condition check
            processed_indices = set()
            for k in flattened_keys:
                try:
                    # This should get us a pd.Series
                    item_to_join = self.get(k)
                except AttributeError:
                    # Cannot get anything for this key. Return default value
                    return default
                else:
                    # Check join condition
                    current_indices = set(item_to_join.index.names)
                    assert (
                        current_indices.issubset(processed_indices) |
                        processed_indices.issubset(current_indices)
                    ), \
                        f'Indices of requested items must be subsets of' + \
                        f' each other. {k} has indices {current_indices}' + \
                        f' but prior indices were {processed_indices}.'
                    # if no error, continue to join with prior results
                    if joined is None:
                        joined = item_to_join.to_frame()
                    else:
                        joined = joined.join(item_to_join)
                    # Add processed indices to set
                    processed_indices |= current_indices
    