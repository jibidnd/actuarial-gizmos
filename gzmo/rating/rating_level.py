import pandas as  pd
from rating_table import RatingTable

class RatingLevel(pd.DataFrame):
    """A class to represent a "level" for rating.
    For example, policies, drivers, vehicles, households, or fleets.
    """

    def __init__(self, *args, **kwargs):
        super().__init__()

    # to retain subclasses through pandas data manipulations
    # https://pandas.pydata.org/docs/development/extending.html
    @property
    def _constructor(self):
        return RatingLevel

    # @property
    # def _constructor_sliced(self):
    #     return RatingAttribute

    # not to be confused with __getattribute__
    # __getattr__ is only invoked when the attribute is
    #   not found in usual ways
    # __getattribute__ is invoked before looking at the actual attribute
    def __getattr__(self, name: str):
        for record in self.records:
            if name in record.columns:
                # if the records are not indexed exactly the same
                #   as self, reindex / loc / xs don't work
                # Using a join (not a merge) is actually pretty quick
                # This is like what one would expect from
                #   record.reindex(self.index)
                return getattr(self.join[[]](record, how = 'left'), name)
        return super().__getattr__(name)

    def add_records(self, records: RatingLevel):
        pass
    
    # TODO: handling missing data

    def apply(arg, **kwargs):
        if isinstance(arg, RatingTable):
            table = arg
            # gather inputs
            for input_ in table.inputs:
                try:
                    # TODO: handle wildcards

        else:
            super().apply(arg, **kwargs)

        if factor, apply, else super().apply

    def get

class RatingAttribute(pd.Series):
    pass