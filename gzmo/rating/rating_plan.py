from abc import ABC, abstractmethod

import pandas as pd
from sympy import comp

from gzmo.rating import rating_table


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
        excel_file = pd.read_excel(*args, **kwargs)
        for table_name, table in excel_file.items():
            rating_table = rating_table.RatingTable(table_name, table)
            self.add_rating_table(table_name, table)
        
        return

    def add_rating_table(self, name: str, rating_table: rating_table) -> None:  
        self.rating_tables[name]= rating_table
        self.add_rating_step(name, rating_table)
        return
    
    def add_rating_step(self, rating_step: RatingStep) -> None:
        self.rating_steps[rating_step.name] = rating_step
        return

    def rate(self, policies, results):
        # each rating table is a rating step
        # and each operation is also a rating step
        for rating_step_name, rating_step in self.rating_steps.items():
            rating_step.rate(policies, results)
        return



class RatingStep(ABC):

    def __init__(self, name) -> None:
        super().__init__()
        self.name = name
    
    @abstractmethod
    def calculate(self, policies, results):
        """Override this method to provide user-defined calculation.
        The method should take *policies* and *results* as arguments,
        and return a factor indexed by identifying characteristics,
        as a dataframe.

        Additional parameters (e.g. hard-coded values) can be added
        to the `RatingStep` instance itself.

        Args:
            policies (RatingLevel): this is a rating_level.RatingLevel
                object that allows access of lower-level attributes
                via the `.` method.
            results (RatingResults): TODO
        """
        pass

    def rate(self, policies, results):
        result = self.calculate(policies, results)
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
        
        # TODO: place here or in rating plan?
        # The operation that should be used to combine
        #   the result of this rating table with the
        #   immediately previous step of the rating plan
        # self.operation = ''



        # format the table
        self.prime()

    # to retain subclasses through pandas data manipulations
    # https://pandas.pydata.org/docs/development/extending.html

    # @property
    # def _constructor(self):
    #     return RatingTable

    # def __call__(self, inputs):
    #     # TODO
    #     # multiple inputs? range inputs?
    #     return self.loc[inputs]

    def prime(self):
        """Formats the inputs and outputs given rating plan information.
        """
        # first identify the inputs and outputs
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
                if c.endswith('_left') or c.endswith('_right'):
                    cleaned = c[1:].replace('_left', '').replace('_right', '')
                    if cleaned not in self.inputs:
                        interval_inputs.append(cleaned)
                        self.inputs.append(cleaned)
                else:
                    # TODO: add warning
                    cleaned = c[1:]
                    other_inputs.append(cleaned)
                    self.inputs.append(cleaned)
            elif c.endswith('_'):
                cleaned = c[:-1]
                outputs.append(cleaned)
                self.outputs.append(cleaned)

        # Create new index
        # Note that interval ranges are assumed to be closed on both ends,
        #  consistent with how rating plans are usually built. 
        new_indices = []
        for c in self.inputs:
            if c in interval_inputs:
                assert ((f'_{c}_left' in self.columns)
                        and (f'_{c}_right' in self.columns)), \
                    f'Missing column for interval input {c}'
                # create pandas interval index
                idx = pd.IntervalIndex.from_arrays(
                    self[f'_{c}_left'], self[f'_{c}_right'],
                    closed = 'left',
                    name=c
                )
                new_indices.append(idx)
            else:
                idx = pd.Index(self[f'_{c}'], name = c)
                new_indices.append(idx)

        # create new dataframe
        self.set_index(new_indices, inplace=True)
        # rename the columns and subset
        self.rename(columns = {f'{c}_': c for c in outputs}, inplace = True)
        self.columns.difference(outputs)
        self.drop(self.columns.difference(outputs), axis = 1, inplace = True)
    

    def calculate(policies, results):
        TODO
        pass




# credit_tier_factor = RatingTable(df_credit_tier_table)
# def rating_steps(book, results):
#     premium = \
#         book.apply(credit_tier_factor) * \
#         results.driver_age_factor * \
#         results.vehicle_age_factor
#     return premium
