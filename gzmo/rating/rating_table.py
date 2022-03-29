import pandas as pd


class RatingTable(pd.DataFrame):

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

    def __init__(self, *args, **kwargs) -> None:
        """Initializes a RatingTable instance.

        Args:
            possible_inputs (list): a list of columns that could be inputs.
            outputs (list): a list of columns that could be outputs.
        """
        super().__init__(*args, **kwargs)

        # input and output columns
        self.inputs = []
        self.outputs = []

        # information about the rating table
        self.name = None
        self.version = None
        self.effective_date = None
        self.serff_filing_number = None
        self.state_tracking_number = None
        self.company_tracking_number = None
        # any additional information to attach to the table
        self.additional_info = {}

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

        # create new index
        new_indices = []
        for c in self.inputs:
            if c in interval_inputs:
                assert ((f'_{c}_left' in self.columns)
                        and (f'_{c}_right' in self.columns)), \
                    f'Missing column for interval input {c}'
                # create pandas interval index
                idx = pd.IntervalIndex.from_arrays(
                    self[f'_{c}_left'], self[f'_{c}_right'],
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
