import pandas as pd

from gzmo.base import SearchableDict
from gzmo.rating.rating_plan import RatingPlan, InterpolatedRatingTable, RatingStep


# read in excel tables
# all tables are automatically added as lookup tables
wgic_01 = RatingPlan.from_excel(r'examples/WGIC/WGIC_rating_manual.xlsx')

# Specify that Amount of Insurance factor should be interpolated
wgic_01.register(
    amount_of_insurance_factor = 
    InterpolatedRatingTable.from_rating_table(
        wgic_01.amount_of_insurance_factor
        )
)

# read in policies
policy_tables = \
    pd.read_excel(r'examples/WGIC/WGIC_policies.xlsx', sheet_name = None)
wgic_policies = SearchableDict(**policy_tables)


def calculate_final_premium(session):
    ret = \
        session.base_rates \
        * session.amount_of_insurance_factor \
        * session.territory_factor \
        * session.protectclass_constr_factor \
        * session.underwriting_tier_factor \
        * session.deductible_factor \
        * session.new_home_discount_factor \
        * session.five_year_claim_free_factor \
        * session.multi_policy_discount_factor
    return ret

wgic_01.register(final_premium = RatingStep(calculate_final_premium))

session = wgic_01.rate(wgic_policies, parallel = True)