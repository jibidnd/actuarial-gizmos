import pandas as pd
from gzmo.base import FancyDict

from gzmo.core import Book
from gzmo.rating.rating_plan import RatingPlan, InterpolatedRatingTable


# initialize an empty rating plan
wgic_01 = RatingPlan('WGIC Rating Plan')
# read in excel tables
# all tables are automatically added as lookup tables
wgic_01.read_excel(r'examples/WGIC_rating_manual.xlsx')

# Specify that Amount of Insurance factor should be interpolated
wgic_01.register(
    amount_of_insurance_factor = 
    InterpolatedRatingTable.from_rating_table(
        wgic_01.amount_of_insurance_factor
        )
)

# read in policies
policy_tables = \
    pd.read_excel(r'examples/WGIC_policies.xlsx', sheet_name = None)
wgic_policies = Book(**policy_tables)

def calculate_final_premium(session):
    ret = \
        session.base_rate \
        * session.new_home_discount
    return ret

wgic_01.register(final_premium = calculate_final_premium)

rated = wgic_01.rate(wgic_policies)
