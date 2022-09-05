import pandas as pd

from gzmo.base import SearchableDict
from gzmo.rating.rating_plan import RatingPlan, RatingStep, InterpolatedRatingTable

from examples.Metromile.NJ_rating_plans.NJ_base import NJ_Base_RatingPlan
from examples.Metromile.make_market_basket import nj06_market_basket

# TODO: run and debug
class NJ06_RatingPlan(NJ_Base_RatingPlan):
    def __init__(self):
        super().__init__()
        self.read_excel(r'examples/Metromile/NJ06.xlsx')

        # convert a few tables to interpolated tables
        # these tables have an "each addiitonal" rule
        for tbl_name in [
            'driving_points_AAF',
            'driving_points_DWI',
            'driving_points_IND',
            'driving_points_MAJ',
            'driving_points_MIN',
            'driving_points_NAF',
            'driving_points_SPD'
        ]:
            self[tbl_name] = InterpolatedRatingTable.from_rating_table(self[tbl_name])

nj06 = NJ06_RatingPlan()

session = nj06.rate(nj06_market_basket)

for k, v in session.rating_results.items():
    if isinstance(v, pd.DataFrame):
        if v.isna().any().any():
            print(k)
            print(v.loc[v.isna().any(axis = 1)])
        else:
            print(f'{k} is good!')
    elif isinstance(v, pd.Series):
        if v.isna().any():
            print(k)
            print(v.loc[v.isna()])
        else:
            print(f'{k} is good!')
    else:
        try:
            print(k)
            print(v[1])
        except:
            print(k)
            print(v)




# (
#     session.driver_classification_factor * \
#     session.experienced_driver_factor + \
#     session.driving_record_points_factor
#     ) * \
# session.household_member_factor * \
# session.driver_age_point_factor * \
# session.credit_clean_driver_factor


# type(    session.driver_classification_factor * session.experienced_driver_factor)

# session.driver_classification_factor * \
# session.household_member_factor
