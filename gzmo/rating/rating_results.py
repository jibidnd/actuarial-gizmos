from xml.dom.minidom import Attr
import pandas as pd
import numpy as np

class RatingResults(dict):
    """A dictionary-like class to host rating results.
    Both intermediate and final rating steps are stored here.
    """

    def __init__(self) -> None:
        super().__init__()

        # TODO: anything to add?

    def __getattr__(self, name: str):
        """Searches and returns an attribute in the result dataframes"""
        for item_name, item in self.items():
            # First check if any dataframe is named `name`
            if item_name == name:
                return item
            else:
                # Otherwise loop through each dataframe's columns
                # to see if we have a column named `name`
                for column in item:
                    if column == name:
                        return item[column]
        
        # If we get here that means we didn't find anything
        # Raise an error
        raise AttributeError(f"Item {name} not found in results.")


    def register(self, name: str, result: pd.DataFrame) -> None:
        # can add checks here
        self[name] = result





# credit_tier_factor = RatingTable(df_credit_tier_table)
# def rating_steps(book, results):
#     premium = \
#         book.apply(credit_tier_factor) * \
#         results.driver_age_factor * \
#         results.vehicle_age_factor
#     return premium
