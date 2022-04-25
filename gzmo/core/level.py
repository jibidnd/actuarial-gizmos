import itertools
import pandas as  pd

class Level(pd.DataFrame):
    """A class to represent a "level" for rating.
        For example, accounts, policies, drivers, vehicles,
        households, or fleets.
        One can also have an empty dataframe as the top
        level to repesent a "book".

    """

    _metadata = ['sublevel', 'records']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Check that index is not duplicated
        assert not self.index.has_duplicates, \
            'Levels are not allowed to have duplicated indices'

        # sublevels should be indexed at least as granular as self
        #   examples include drivers or vehicles under a policy
        self.sublevels = {}
        

    # to retain subclasses through pandas data manipulations
    # https://pandas.pydata.org/docs/development/extending.html
    @property
    def _constructor(self):
        return Level

    # Tossing this code but may recycle later
    # def __getattr__(self, name: str):
    #     """Searches and returns an attribute in self, self.sublevels,
    #         or self.records.
    #         If `name` is an attribute of one of the records in
    #         `self.records`, the attribute returned will be reindexed
    #         to be the same as `self`.
    #         In the case where there are nested records, the attribute
    #         returned should be indexed to be the same as the top of the
    #         chain of records.


    #         Order of resolution:
    #             1. If `name` is in `self.sublevels`, return the sublevel.
    #             2. If `name` is an attribute of a sublevel,
    #                 return that attribute. `sublevel`s are searched
    #                 in insertion order.
    #             3. If `name` is in `self.records`, return the records.
    #             4. If `name` is an attribute of a set of records,
    #                 return that attribute. `records` are searched
    #                 in insertion order.
        
    #     Args:
    #         name (str): The name of the attribute to search for.

    #     Returns:
    #         RatingLevel or RatingAttribute: The attribute searched for.
        
    #     Raises:
    #         AttributeError: If the attribute cannot be found.
    #     """

    #     # Look at sublevels
    #     for sublevel_name, sublevel in self.sublevels.items():
    #         # 1. Is `name` a sublevel?
    #         if sublevel_name == name:
    #             return sublevel
    #         # 2. Is `name` an attribute of a sublevel?
    #         else:
    #             try:
    #                 return getattr(sublevel, name)
    #             except AttributeError:
    #                 pass
        
    #     # Look at records
    #     for record_name, record in self.records.items():
    #         # 3. Is `name` a record?
    #         if record_name == name:
    #             return record
    #         # 4. Is `name` an attribute of a record?
    #         else:
    #             try:
    #                 # if the sublevels are not indexed exactly the same
    #                 #   as self, reindex / loc / xs don't work
    #                 # Using a join (not a merge) is actually pretty quick
    #                 # This is like what one would expect from
    #                 #   sublevel.reindex(self.index)
    #                 return getattr(self[[]].join(sublevel, how = 'left'), name)
    #             except AttributeError:
    #                 pass
        
    #     return super().__getattr__(name)
    
    
    # not to be confused with __getattribute__
    # __getattr__ is only invoked when the attribute is
    #   not found in usual ways
    # __getattribute__ is invoked before looking at the actual attribute
    def __getattr__(self, name: str):
        """Searches and returns an attribute in self or self.sublevels.

            Order of resolution:
                1. If `name` is in `self.sublevels`, return the sublevel.
                2. If `name` is an attribute of a sublevel,
                    return that attribute. `sublevel`s are searched
                    in insertion order.
        
        Args:
            name (str): The name of the attribute to search for.

        Returns:
            RatingLevel or RatingAttribute: The attribute searched for.
        
        Raises:
            AttributeError: If the attribute cannot be found.
        """
        # First see if the dataframe returns anything
        try:
            return getattr(super(), name)
        except AttributeError:
            # If the dataframe doesn't return anything, go to our `get` method
            if (ret :=  self.get(name)) is not None:
                return ret
            else:
                raise AttributeError(f'Attribute {name} cannot be found.')


    
    def get(self, key, default = None):
        """Overrides panda's `.get`.
            Get item(s) given key(s) (ex: DataFrame column(s)).
            Returns default value if not found.

            This is handy when we want to get a list of columns
            that are at different levels (e.g. credit score from
            policy level and vehicle age from vehicle level). Simply
            pass a list as key and the method will return the requested
            items indexed at the highest level.

            # TODO: what happens if a column uses something other
            #   than a string as a column name?
            # TODO: check index levels of join

        Args:
            key (object): The key identifying the object to return
            default (object, optional): Object to return if`key` is
                not found. Defaults to None.
        """
        # First see if pandas returns anything
        if ret:= super().get(key) is not None:
            return ret
        # If not, also check sublevels
        else:
            for sublevel_name, sublevel in self.sublevels.items():
                # 1. Is `key` a sublevel?
                if sublevel_name == key:
                    return sublevel
                # 2. Is `key` an attribute of a sublevel?
                elif (ret := sublevel.get(key)) is not None:
                    return ret
        
        # if the key is a single string,
        # getting here means we can't find it
        if isinstance(key, str):
            return default
        
        # If we get here, the key does not belong to the current level
        #   or any single sublevel. Need to do some joinery.
        joined = None
        try:
            iterator = iter(key)
        except TypeError as e:
            # If the key is not iterable, it is a single key
            #   that cannot be found.
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
            
            return joined


    def add_sublevels(self, name: str, sublevel: Level):
        self.sublevels[name] = sublevel
    
    def add_records(self, name: str, records: Level):
        self.records[name] = records
    
    # TODO: handling missing data

    # def apply(arg, **kwargs):
    #     if isinstance(arg, RatingTable):
    #         table = arg
    #         # gather inputs
    #         for input_ in table.inputs:
    #             try:
    #                 # TODO: handle wildcards

    #     else:
    #         super().apply(arg, **kwargs)

    #     if factor, apply, else super().apply

    # def get

# class RatingAttribute(pd.Series):
#     pass

