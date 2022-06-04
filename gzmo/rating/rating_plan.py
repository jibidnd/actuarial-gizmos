from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd
import numpy as np

from gzmo import core
from gzmo import rating


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

    def rate(self, info: core.utils.Info):
        # each rating table is a rating step
        # and each operation is also a rating step
        for rating_step_name, rating_step in self.rating_steps.items():
            rating_step.rate(info)
        return

class RatingStep(ABC):

    def __init__(self, name) -> None:
        super().__init__()
        self.name = name
    
    @abstractmethod
    def evaluate(self, info: core.utils.Info):
        """Override this method to provide user-defined evaluation.
        The method should take `info` as the only argument,
        and return a dataframe indexed by identifying characteristics.

        Additional parameters (e.g. hard-coded values) can be added
        to the `RatingStep` instance itself as attributes.

        Args:
            info (core.utils.Info): This is a class that allows
                access to book info and any rated resuls.
        """
        pass

    def rate(
        self,
        info: core.utils.Info,
        results: rating.rating_results.RatingResults
        ):
        result = self.evaluate(info)
        assert isinstance(result, pd.DataFrame), \
                f"RatingStep {self.name}'s `calculate` method must" + \
                "return a factor as a pandas dataframe."
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
        'outputs'
    ]

    def __init__(
            self,
            name: str,
            df: pd.Dataframe,
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
        super().__init__(data = df.copy(), **kwargs)
        
        # information about the rating table
        self.name = name
        self.version = version
        self.effective_date = effective_date
        self.serff_filing_number = serff_filing_number
        self.state_tracking_number = state_tracking_number
        self.company_tracking_number = company_tracking_number
        # any additional information to attach to the table
        self.additional_info = additional_info or {}

        # input and output columns
        self.inputs = []
        self.outputs = []
        # All possible input values as {input: set(values)}
        # Excludes wildcards
        self.possible_input_values = dict()
        # TODO: clean up
        # # The min and max possible values for interaval inputs
        # # Formatted as {input: (min, max)}
        # self.possible_input_ranges = dict()

        # Whether this table uses wildcards
        self._has_wildcards = False
        # Keep a marker of whether a row has any wildcards
        # For intervals, (-inf, inf) or (*, *) are treated as wildcards
        self._wildcard_markers = pd.DataFrame(index = range(len(self)))

        # format the table
        self.prime()

    # to retain subclasses through pandas data manipulations
    # https://pandas.pydata.org/docs/development/extending.html

    # @property
    # def _constructor(self):
    #     return RatingTable

    def __call__(self, inputs: list):
        """Returns 

        Args:
            inputs (list of lists): The inputs to process.
                Each interior list is interpreted as a set of inputs.

        Returns:
            pd.DataFrame: A DataFrame of the corresponding rows.
        """

        def format_sublist_input(sublist_inputs):

            # First check that the lenth of inputs is appropriate
            assert len(sublist_inputs) == len(self.inputs), \
                f'{self.name} requires inputs {self.inputs}' + \
                f' but received inputs {sublist_inputs}.' + \
                f' Note that each set of inputs must be' + \
                f' passed as a tuple.'

            # Process the inputs if there are wildcards
            if self._has_wildcards:
                # initialize list for formatted inputs
                formatted_inputs = []
                # loop through each input to to replace them with something else
                # if the input is not in the possible values
                for input_name, passed_input in \
                    zip(self.inputs, sublist_inputs):
                    
                    possible_inputs = \
                        self.possible_input_values[input_name]
                    is_interval_input = isinstance(
                        self.index.get_level_values(input_name),
                        pd.IntervalIndex)
                       
                    if is_interval_input:
                        pass
                        # TODO: clean up
                        # in_any_range = \
                        #     any([passed_input in rng
                        #         for rng in possible_inputs])
                        # # -np.inf is used as a marker for wildcards
                        # replacement_value = -np.inf
                        
                        # if in_any_range:
                        #     formatted_inputs.append(passed_input)
                        # else:
                        #     # Find a value that is larger than the max
                        #     # this ensures that the modified input
                        #     # will not match any of the non-wildcard intervals
                        #     formatted_inputs.append(replacement_value)
                    else:
                        if passed_input in possible_inputs:
                            formatted_inputs.append(passed_input)
                        else:
                            formatted_inputs.append('*')
            else:
                formatted_inputs = sublist_inputs
            return tuple(formatted_inputs)


        # Format the tuple
        formatted_inputs = [format_sublist_input(i) for i in inputs]
        # print(formatted_inputs)
        # First try the rows with no wildcards
        non_wildcard_rows = (~self._wildcard_markers).all(axis = 1)
        first_pass = self.loc[non_wildcard_rows].reindex(formatted_inputs).values
        # RESUME HERE
        second_pass = []
        for out in first_pass:
            if np.isnan(out).all():

            else:
                second_pass.append(out)

        # if there are input rows with no matching lookup columns,
        # look in the wildcards
        no_match = first_pass.isna().all(axis = 1)
        # print(no_match)
        # no_match = np.isnan(first_pass).all(axis = 1)
        if not no_match.any(axis = 0):
            return first_pass.values
        else:
            dict_outputs = 
            wildcard_rows = self.loc[~non_wildcard_rows]
            second_pass = wildcard_rows.reindex(
                no_match.loc[no_match].index.map(lambda x: get_idx(x, wildcard_rows)))
            print(first_pass)
            print(no_match)
            print(second_pass)


    def prime(self):
        """Formats the inputs and outputs given rating plan information.

        This creates a pd.MultiIndex for the dataframe. A MultiIndex
            allows using multiple inputs. To keep things consistent,
            we will use a MultiIndex regardless of how many inputs
            there are.
        """

        # first identify the inputs and outputs
        # Also take care of wildcards
        # All inputs start wtih "_"
        # interval inputs start with "_" and have two columns ending
        #   with "_left" and "_right"
        # outputs end with "_"
        # We'd like to preserve order so looping is the easiest
        interval_inputs = []
        other_inputs = []
        outputs = []
        for c in self.columns:
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
                    self.inputs.append(cleaned)
                    # Convert wildcards to -inf and
                    # cast to float for performance
                    self[c] = self[c].replace('*', -np.inf).astype(float)
                elif c.endswith('_right'):
                    # _right columns don't need to be added to input list
                    self[c] = self[c].replace('*', np.inf).astype(float)
                # Other inputs
                else:
                    # add to input list
                    cleaned = c[1:]
                    other_inputs.append(cleaned)
                    self.inputs.append(cleaned)
                    # Make a note of wildcard usage, if any
                    if (self[c]=='*').any():
                        self._has_wildcards = True
            elif c.endswith('_'):
                # add outputs to the list of outputs
                cleaned = c[:-1]
                outputs.append(cleaned)
                self.outputs.append(cleaned)
        # Create new index
        # Note that interval ranges are assumed to be closed on both ends,
        #  consistent with how rating plans are usually built. 
        new_indices = []
        for c in self.inputs:
            if c in interval_inputs:
                # Check that if interval, both _left and _right are defined
                assert ((f'_{c}_left' in self.columns)
                        and (f'_{c}_right' in self.columns)), \
                    f'Missing column for interval input {c}'
                # Check for "double wildcard" cases
                # i.e. left == '*' and right == '*'
                double_wildcard = \
                    (self[f'_{c}_left'] == -np.inf) & \
                    (self[f'_{c}_right'] == np.inf)
                # # Also take note of "single wildcard" cases
                # # i.e. one end of the interval is open ended
                # single_wildcard = \
                #     (self[f'_{c}_left'] == -np.inf) != \
                #     (self[f'_{c}_right'] == np.inf)
                # # A table cannot have both single wildcards and
                # # double wildcards
                # # e.g. one row with (30, *)
                # # and another row with (*, *)
                # # since that is very ambiguous
                # assert not (any(single_wildcard) & any(double_wildcard)), \
                #     'A table with double wildcards (intervals with * as both' + \
                #     ' left and right ends) cannot also have single wildcards' + \
                #     ' (one of the left or right ends being a *).'
                # # for double wildcards, replace the right value to -np.inf
                # # any passed inputs not falling into other (non-wildcard)
                # # intervals will be assigned a value of -np.inf
                # # This is similar to '*' for other input types
                # self[f'_{c}_right'].loc[double_wildcard] = -np.inf
                # create pandas interval index
                idx = pd.IntervalIndex.from_arrays(
                    self[f'_{c}_left'], self[f'_{c}_right'],
                    closed = 'both',
                    name=c
                )
                # TODO: assert not overlapping?
                new_indices.append(idx)
                # # keep track of min and max accepted values
                # min_ = min([i.left for i in idx if i != -np.inf])
                # max_ = max([i.right for i in idx if i != np.inf])
                # self.possible_input_ranges[c] = (min_, max_)
            else:
                idx = pd.Index(self[f'_{c}'], name = c)
                new_indices.append(idx)
            # keep track of all possible input values
            self.possible_input_values[c] = set(idx)
        
        # create new dataframe
        # Cannot use self.set_index here as that apparently
        #   flattens the multiindex if it only has one level.
        #   See source code for details.
        #   I don't understand it that well.
        self.index = pd.MultiIndex.from_arrays(new_indices)
        # keep track of wildcard usage
        wildcard_characters = [
            pd.Interval(-np.inf, np.inf, closed = 'both'),
            '*'
        ]
        self._wildcard_markers = self.index.to_frame().isin(wildcard_characters)
        # rename the columns
        self.rename(columns = {f'{c}_': c for c in outputs}, inplace = True)
        # Is there a way to keep certain columns?
        # We can't assign the result to another variable
        self.drop(self.columns.difference(outputs), axis = 1, inplace = True)
    

    def evaluate(self, info: core.utils.Info):
        """This method will search for the input columns from info,
        and return the appropriate output rows.

            For rating table with wildcrads, it will perform an additional
                step. Essentially, it checks whether each input is in the
                non-wildcard options in the rating table. If it is, it uses
                the unmodified input value to find a match. Otherwise, it
                uses '*' to find a match.

        Args:
            info (core.utils.Info): This is a class that allows
                access to book info and any rated resuls.
        """

        # rating table must have multi index, even if just 1 column
        # then we can do self.reindex(df_inputs.to_records(index = False).tolist())
        
        # Gather the inputs
        # May raise if info cannot get all of self.inputs,
        # or if inputs are on incompatible indices (cannot be joined)
        inputs_to_process = info.get(self.inputs)
        # Process the inputs if there are wildcards
        if self._has_wildcards:
            for input_name in self.inputs:
                
                possible_inputs = self.possible_input_values[input_name]
                is_interval_input = isinstance(
                    self.index.get_level_values(input_name), pd.IntervalIndex)
                
                if is_interval_input:
                    in_any_range = inputs_to_process[input_name].map(
                        lambda x: any([x in rng for rng in possible_inputs]))
                    # -np.inf is used as a marker for wildcards
                    replacement_value = -np.inf
                    inputs_to_process[input_name] = \
                        inputs_to_process[input_name].where(
                            in_any_range, replacement_value)
                
                else:
                    matches_any_value = \
                        inputs_to_process[input_name].isin(possible_inputs)
                    # If an input is not in the non-wildcard set,
                    #   replace it with '*'
                    replacement_value = '*'
                    inputs_to_process[input_name] = \
                        inputs_to_process[input_name].where(
                        matches_any_value, replacement_value)
                
        # get and return the outputs
        return self.reindex(inputs_to_process.to_records(index = False).tolist())

def get_idx(passed_inputs, df_lookup):
    wildcard_characters = [
            pd.Interval(-np.inf, np.inf, closed = 'both'),
            '*'
        ]
    matches = []
    for passed_input, lookup_column in zip(passed_inputs, df_lookup.columns):
        matches.append(
            df_lookup[lookup_column].index.to_frame().isin(
                wildcard_characters + [passed_input]
            )
        )
    df_matches = pd.concat(matches, axis = 1)
    try:
        matched = df_matches.loc[df_matches.all(axis = 1)].index[0]
    except IndexError:
        matched = None
    return matched
    






#TODO: add "slow" method to take care of wildcards that are not "everything other than possible values" 
# but "anything"
# def get_row_number(passed_inputs):
#     subset = subset of rating table where any column has * or [-inf,-inf]
#     true_counter = []
#     for input_name in self.inputs:
#         s = (self[input_name] == passed_inputs[input_name]) | \
#             (self[input_name] == '*' (or -inf))
#         true_counter.append(s)
#     all_true = pd.DataFrame(true_counter).all(axis = 1)
#     selected_row =self.index[all_true].iloc[0]
