from __future__ import annotations

from abc import ABC, abstractmethod
from cgitb import lookup
from typing import Union

import pandas as pd
import numpy as np
from py import process
from sklearn import utils

from gzmo import core
from gzmo import rating
from gzmo.rating import helpers


class RatingPlan:
    """This class handles the initialization of a rating plan."""

    def __init__(self, name: str) -> None:
        
        self.name = name
        self.rating_tables = {}
    
    def read_excel(self, io: str, *args, **kwargs) -> None:
        """A wrapper of panda's read_excel function that
            - reads and prepare rating tables from the excel file
            - adds the tables to the rating plan.

        Args:
            See pandas.read_excel documentation for details.
        """
        excel_file = pd.read_excel(io, *args, **kwargs)
        for table_name, table in excel_file.items():
            rating_table = RatingTable(table_name, table)
            self.add_rating_table(table_name, table)
        return

    def add_rating_table(self, name: str, rating_table: RatingTable) -> None:  
        self.rating_tables[name]= rating_table
        self.add_rating_step(name, rating_table)
        return
    
    def add_rating_step(self, rating_step: RatingStep) -> None:
        self.rating_steps[rating_step.name] = rating_step
        return

    def rate(self, book: core.Book):
        # each rating table is a rating step
        # and each operation is also a rating step
        for rating_step_name, rating_step in self.rating_steps.items():
            rating_step.rate(book)
        return

class RatingStep(ABC):

    def __init__(self, name) -> None:
        super().__init__()
        self.name = name
    
    @abstractmethod
    def evaluate(self, *args, **kwargs):
        """Override this method to provide user-defined evaluation.

        This should return a dataframe as the resulting factor/variable.

        Additional parameters (e.g. hard-coded values) can be added
        to the `RatingStep` instance itself as attributes.
        """
        pass

    def rate(
        self,
        book: core.Book,
        results: rating.rating_results.RatingResults = None
        ):
        result = self.evaluate(book)
        if results:
            results[self.name] = result
        return


class RatingTable(pd.DataFrame, RatingStep):

    # _metadata define properties that will be passed to manipulation results.
    # https://pandas.pydata.org/docs/development/extending.html
    _metadata = [
        'name',
        'version',
        'effective_date',
        'serff_filing_number',
        'state_tracking_number',
        'company_tracking_number',
        'additional_info',
        'inputs',
        'outputs',
        'wildcard_characters',
        '_wildcard_markers'
    ]

    def __init__(
            self,
            data: pd.Dataframe,
            name: str,
            inputs: list,
            outputs: list,
            wildcard_characters = None,
            version: str = None,
            effective_date: str = None,
            serff_filing_number: str = None,
            state_tracking_number: str = None,
            company_tracking_number: str = None,
            additional_info: dict = None,
            **kwargs
            ) -> None:
        """Initializes a RatingTable instance.

        Args:
            name (str): The name of the rating table.
            version (str, optional): A user-defined version number for
                the rating table. Defaults to None.
            effective_date (str, optional): The effective date of the
                rating table. Defaults to None.
            serff_filing_number (str, optional): The SERFF filing number
                of the rating table. Defaults to None.
            state_tracking_number (str, optional): The State Tracking Number
                of the rating table. Defaults to None.
            company_tracking_number (str, optional): The Company Tracking
                Number of the rating table. Defaults to None.
            additional_info (dict, optional): Any additional info to be
                attached to the rating table. Defaults to None.
        """
        super().__init__(data = data.copy(), **kwargs)
        
        # information about the rating table
        self.name = name
        self.inputs = inputs
        self.outputs = outputs
        self.wildcard_characters = \
            wildcard_characters or \
            ['*', pd.Interval(-np.inf, np.inf, closed = 'both')]
        self.version = version
        self.effective_date = effective_date
        self.serff_filing_number = serff_filing_number
        self.state_tracking_number = state_tracking_number
        self.company_tracking_number = company_tracking_number
        # any additional information to attach to the table
        self.additional_info = additional_info or {}


        # Keep a marker of whether a row has any wildcards
        # For intervals, (-inf, inf) or (*, *) are treated as wildcards
        self._wildcard_markers = helpers.get_wildcard_markers(
            self.index.to_frame(),
            self.wildcard_characters
            )

        return        
        

    # to retain subclasses through pandas data manipulations
    # https://pandas.pydata.org/docs/development/extending.html
    # Also see https://github.com/pandas-dev/pandas/issues/19300
    @property
    def _constructor(self):
        return RatingTable._internal_ctor

    @classmethod
    def _internal_ctor(cls, *args, **kwargs):
        kwargs['name'] = None
        kwargs['inputs'] = None
        kwargs['outputs'] = None
        return cls(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        return self.evaluate(*args, **kwargs)

    @classmethod
    def from_unprocessed_table(cls, df, name, **kwargs):
        processed_rating_table, inputs, outputs = \
            helpers.process_rating_table(df)
        return cls(processed_rating_table, name, inputs, outputs, **kwargs)

    # TODO: use typing.overload for type hinting

    def evaluate(self, *args, **kwargs):
        # calls different methods based on input type.
        if args:
            if isinstance(args[0], dict):
                return self._lookup(**args[0])
            elif isinstance(args[0], pd.DataFrame):
                inputs = args[0].get(self.inputs)
                return self._eval_impl(inputs)
            else:
                return self._lookup(*args)
        elif kwargs:
            return self._lookup(**kwargs)

    def _eval_impl(self, input_table):
        # The function to handle dataframe inputs
        # first do _eval_reindex on non-wildcard rows of self for all rows in input_table
        # if fails, just fill in with None
        # then do eval_match on wildcard rows on any rows that do not yet have a value
        
        # if there are no inputs, simply do a cross join
        if self.inputs == []:
            assert len(self) == 1, \
                f'{self.name} has no inputs but has more than 1 row.'
            empty_dataframe = pd.DataFrame(index = input_table.index)
            ret = empty_dataframe \
                .assign(temp_join_key = 1) \
                .merge(
                    self.assign(temp_join_key = 1),
                    on = 'temp_join_key',
                    how = 'left'
                    ) \
                .drop('temp_join_key', axis = 1)
            return ret


        # first use _eval_reindex on non-wildcard rows of self
        lookup_table_nonwildcard = self.loc[(~self._wildcard_markers).values]
        # need to remove the unused levels becuase they somehow mess up reindexing
        # See https://pandas.pydata.org/docs/user_guide/advanced.html#defined-levels
        lookup_table_nonwildcard.index = \
            lookup_table_nonwildcard.index.remove_unused_levels()
        try:
            res_nonwildcard = self._eval_reindex(
                input_table, lookup_table = lookup_table_nonwildcard)
        # if reindex throws an error, go straight to using _eval_match
        except:
            res = self._eval_match(
                input_table, lookup_table = self)
            return res
        else:
            # use self._eval_match to match on wildcard rows
            lookup_table_wildcard = self.loc[self._wildcard_markers]
            isna = res_nonwildcard.isna().all(axis = 1)
            to_lookup = input_table.loc[isna]
            res_wildcard = self._eval_match(
                to_lookup, lookup_table = lookup_table_wildcard)
            # combine res_nonwildcard and res_wildcard
            res = res_nonwildcard.fillna(res_wildcard)
            # res = pd.concat([res_nonwildcard, res_wildcard], axis = 0, ignore_index = False)
            # reorder to original index
            res = res.loc[input_table.index]
            return res
        

    def _eval_reindex(self, input_table, lookup_table = None):
        """Uses df.reindex to look up rows that match 
            each row of inputs in input_table.

        Args:
            input_table (pd.DataFrame): A Pandas DataFrame containing the
                inputs to be processed, with columns named like self.inputs.
            lookup_table: The dataframe in which to lookup rows.

        Returns:
            pd.DataFrame: Rows in `self` with index matching the inputs.
        
        Raises:
            ValueError: pandas multiindices do not play well with overlapping
                intervals in the IntervalIndex, even if the multiindex itself
                is unique. If there are rows with overlapping intervals,
                may raise:
                `ValueError: setting an array element with a sequence.`
        """

        # reorder input columns
        passed_inputs = input_table.loc[:, self.inputs]

        # Define the lookup table
        if lookup_table is None:
            lookup_table = self

        # perform the lookup
        inputs = passed_inputs.to_records(index = False).tolist()
        results = lookup_table.reindex(
            index = inputs, columns = self.outputs, copy = True)
        # use the inputs' index
        # We also want to create a new dataframe, otherwise the resulting
        # object would be a RatingTable instance.
        results = pd.DataFrame(results, index = passed_inputs.index)
        return results

        # except ValueError as err:
        #     if str(err) == 'setting an array element with a sequence.':
        #         raise Exception('')

    def _eval_match(self, input_table, lookup_table = None):
        """Uses self._lookup to look up rows that match 
            each row of inputs in input_table.

        Args:
            input_table (pd.DataFrame): A Pandas DataFrame containing the
                inputs to be processed, with columns named like self.inputs.
            lookup_df: The dataframe in which to lookup rows.

        Returns:
            pd.DataFrame: Rows in `self` with index matching the inputs.
        """

        # Define the lookup table
        if lookup_table is None:
            lookup_table = self
        records = input_table.apply(
            lambda row: self._lookup(
                lookup_table = lookup_table, **row.to_dict()
                ),
            axis = 1
            ).tolist()
        # keep original index
        result = pd.DataFrame.from_records(records, index = input_table.index)
        
        return result

    def _lookup(self, *args, lookup_table = None, **kwargs):
        """Function to handle a single (set of) input.
        Pass one set of inputs to retrieve a mathching row as a dict.

        Args:
            *args: Inputs passed as non-keyword arguments must match the
                order of `self.inputs`. Keyword arguments are ignored
                if any non-keyword arguments are passed.
            lookup_table: The dataframe in which to lookup rows.
            **args: Inputs passed as keyword arguments must match
                `self.inputs` exactly. Keyword arguments are ignored
                if any non-keyword arguments are passed.
        Returns:
            dict: {input name: output value}
        """
        
        # Define the lookup table
        if lookup_table is None:
            lookup_table = self

        # if there ar eno inputs, simply return the (first) row
        if self.inputs == []:
            return lookup_table.iloc[0].to_dict()

        # First convert non-keyword arguments to a dict
        if args:
            passed_inputs = {k: v for k, v in zip(self.inputs, args)}
        else:
            passed_inputs = {**kwargs}


        # Find out what rows match the given inputs
        matches = []
        for input_name in self.inputs:
            passed_input = passed_inputs[input_name]
            lookup_column = lookup_table.index.get_level_values(input_name)
            if isinstance(lookup_column, pd.IntervalIndex):
                matched = lookup_column.contains(passed_input)
            else:
                matched = lookup_column.isin(
                    self.wildcard_characters + [passed_input]
                )
            matches.append(matched)
        
        matching_rows_filter = np.all(matches, axis = 0)
        matching_rows = lookup_table.loc[matching_rows_filter]
        if len(matching_rows) == 0:
            return {k: None for k in self.inputs}
        else:
            return matching_rows.iloc[0].to_dict()