import pandas as  pd

class rating_table(pd.DataFrame):

    # additional properties
    __metadata = [
        'name',
        'version',
        'effective_date',
        'serff_filing_number',
        'state_tracking_number',
        'company_tracking_number'
    ]


    def __init__(self) -> None:
        pass
