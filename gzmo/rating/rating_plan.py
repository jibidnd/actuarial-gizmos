from abc import ABC, abstractmethod

import pandas as pd
import numpy as np


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
            rating_table = rating_table.RatingTable(table_name, table)
            self.add_rating_table(table_name, table)
        return

    def add_rating_table(self, name: str, rating_table: RatingTable) -> None:  
        self.rating_tables[name]= rating_table
        self.add_rating_step(name, rating_table)
        return
    
    def add_rating_step(self, rating_step: RatingStep) -> None:
        self.rating_steps[rating_step.name] = rating_step
        return

    def rate(self, book, results):
        # each rating table is a rating step
        # and each operation is also a rating step
        for rating_step_name, rating_step in self.rating_steps.items():
            rating_step.rate(book, results)
        return

class RatingStep(ABC):

    def __init__(self, name) -> None:
        super().__init__()
        self.name = name
    
    @abstractmethod
    def evaluate(self, book, results):
        """Override this method to provide user-defined evaluation.
        The method should take *book* and *results* as arguments,
        and return a dataframe indexed by identifying characteristics.

        Additional parameters (e.g. hard-coded values) can be added
        to the `RatingStep` instance itself.

        Args:
            book (Level): this is a core.Level
                object that allows access of lower-level attributes
                via the `.` method.
            results (RatingResults): this is a rating_results.RatingResults
                object that hosts intermediate variables and factors
                that have been determined.
        """
        pass

    def rate(self, book, results):
        result = self.evaluate(book, results)
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
            *args,
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
        super().__init__(*args, **kwargs)
        
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
        self.possible_input_values = {}

        # Whether this table uses wildcards
        # Note that wildcard in ranges are converted to -infs and infs,
        #   and thus don't count as wildcards.
        self._has_wildcards = False

        # format the table
        self.prime()

    # to retain subclasses through pandas data manipulations
    # https://pandas.pydata.org/docs/development/extending.html

    # @property
    # def _constructor(self):
    #     return RatingTable

    def __call__(self, inputs: list or tuple):
        """Returns 

        Args:
            inputs (tuple or list of tuples): The inputs to
                process.
                If a tuple is passed, it is interpreted as one
                    set of inputs.
                If a list of tuples is passed, it is interpreted
                    as multiple sets of inputs

        Returns:
            pd.DataFrame: A DataFrame of the corresponding rows.
        """

        def format_tuple(tuple_inputs):

            # First check that the lenth of inputs is appropriate
            assert len(inputs) == len(self.inputs), \
                f'{self.name} requires inputs {self.inputs}' + \
                f' but received inputs {tuple}.'

            # Process the inputs if there are wildcards
            if self._has_wildcards:
                # initialize list for formatted inputs
                formatted_inputs = []
                for input_name, passed_input in zip(self.inputs, tuple_inputs):
                    table_inputs = self.index.get_level_values(input_name)
                    # Nothing to do if this is an interval input,
                    #   since there are no wildcards for interval inputs.
                    if isinstance(table_inputs, pd.IntervalIndex):
                        formatted_inputs.append(passed_input)
                    else:
                        if passed_input in table_inputs:
                            formatted_inputs.append(passed_input)
                        else:
                            formatted_inputs.append('*')
                    formatted_inputs = tuple(formatted_inputs)
            else:
                formatted_inputs = inputs
            return formatted_inputs


        if isinstance(inputs, tuple):
            # Simply wrap the tuple in a list and call the method again
            return self([formatted_inputs])
        else:
            # Format the tuple
            formatted_inputs = [format_tuple(i) for i in inputs]
            # Return the selected rows
            return self.reindex(formatted_inputs)


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
                # create pandas interval index
                idx = pd.IntervalIndex.from_arrays(
                    self[f'_{c}_left'], self[f'_{c}_right'],
                    closed = 'both',
                    name=c
                )
                new_indices.append(idx)
            else:
                idx = pd.Index(self[f'_{c}'], name = c)
                new_indices.append(idx)
            # keep track of all possible input values
            self.possible_input_values[c] = set(idx)

        # create new dataframe
        self.set_index(pd.MultiIndex.from_arrays(new_indices), inplace=True)
        # rename the columns
        self.rename(columns = {f'{c}_': c for c in outputs}, inplace = True)
        # Is there a way to keep certain columns?
        # We can't assign the result to another variable
        self.drop(self.columns.difference(outputs), axis = 1, inplace = True)
    

    def evaluate(self, book, results):
        """This method will search for the input columns from book
            and results, and return the appropriate output rows.

            For rating table with wildcrads, it will perform an additional
                step. Essentially, it checks whether each input is in the
                non-wildcard options in the rating table. If it is, it uses
                the unmodified input value to find a match. Otherwise, it
                uses '*' to find a match.

        Args:
            book (Level): this is a core.Level
                object that allows access of lower-level attributes
                via the `.` method.
            results (RatingResults): this is a rating_results.RatingResults
                object that hosts intermediate variables and factors
                that have been determined.
        """

        # rating table must have multi index, even if just 1 column
        # then we can do self.reindex(df_inputs.to_records(index = False).tolist())
        
        # Gather the inputs
        # Need to loop because Level.get only returns something
        #   if all the keys are present
        inputs_to_process = None
        for i in self.inputs:
            input_to_process = book.get(i) or results.get(i)
            if input_to_process is not None:
                if inputs_to_process is None:
                    inputs_to_process = input_to_process
                else:
                    inputs_to_process = \
                        inputs_to_process.join(input_to_process)
            else:
                raise AttributeError(
                    f'Input {i} not found.')
        # Process the inputs if there are wildcards
        if self._has_wildcards:
            for i in self.inputs:
                # Nothing to do if this is an interval input,
                #   since there are no wildcards for interval inputs.
                if isinstance(self.index.get_level_values(i), pd.IntervalIndex):
                    pass
                else:
                    # If an input is not in the non-wildcard set,
                    #   replace it with '*'
                    inputs_to_process[i] = inputs_to_process[i].where(
                        inputs_to_process[i].isin(self.possible_input_values[i]),
                        '*'
                    )
        # get and return the outputs
        return self.reindex(inputs_to_process.to_records(index = False).tolist())
            
