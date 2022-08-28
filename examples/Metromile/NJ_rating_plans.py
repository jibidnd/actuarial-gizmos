import pandas as pd

from gzmo.base import SearchableDict, AccessLogger
from gzmo.rating.rating_plan import RatingPlan, RatingStep


# initialize an emtpy rating plan
nj06 = RatingPlan.from_excel(r'examples/Metromile/NJ06.xlsx')

# book = SearchableDict()
# book.register(
#     policies = df_policies,
#     drivers = df_drivers,
#     vehicles = df_vehicles
# )
# session = SearchableDict()

# session.register(book = book)


def get_clean_driver(session):
    # Rule D03
    #   Clean Driver Classification –
    #       Drivers with zero BI/PD points are classified as “Clean” drivers.
    return session.get(['BI_points', 'PD_points']).min() == 0
def get_ETBR(session):
    return ~session.drivers['list_only']
def get_pni_age(session):
    return session.drivers.query('is_primary')['age']
def get_pni_marital_status(session):
    return session.drivers.query('is_primary')['marital_status']
def get_pni_gender(session):
    return session.drivers.query('is_primary')['gender']
def get_pni_youthful(session):
    # Rule D01: The term “youthful” refers to drivers under the age of 21.
    youthful_age = 21
    return session.drivers.query('is_primary')['age'] < youthful_age
def get_max_driver_age(session):
    return session.drivers.age.max()
def get_household_member_count(session):
    # Rule D01
    #   Under certain circumstances, the applicant may designate specific
    #   household members as "list only" drivers...
    return session.drivers.groupby('policy_id').size()
def get_ETBR_driver_count(session):
    # Rule D01:
    #   The term "eligible to be rated driver" refers to resident relatives
    #   of legal driving age and drivers of insured vehicles other than those
    #   designated as list only
    return session.ETBR.groupby('policy_id').size()
def get_multiple_ETBR_drivers(session):
    return session.ETBR_driver_count > 1
def get_rated_driver_count(session):
    # Rule D01:
    #   The term “rated driver” refers to driver(s) used to develop the
    #   Household Risk Factor, as defined in the Household Risk Factor
    #   Algorithm.
    # HOUSEHOLD RISK FACTOR RULE
    #     1. RANK all eligible to be rated drivers in household DESCENDING
    #           based on BI Developed Driver Factors
    #     2. SELECT highest ranked drivers up to the number of vehicles
    #     3. AVERAGE Developed Driver Factors for each coverage
    #     4. APPLY to all vehicles
    return session.get(['ETBR_driver_count', 'vehicle_count']).min()
def get_youthful_driver_count(session):
    # Rule P29
    #   The “Youthful Driver Count” counts eligible to be rated,
    #   excluded and list only “youthful” drivers (rule D01).
    return \
        session.drivers.query('ETBR and age <= 21').groupby('policy_id').size()
def get_rated(session):
    bi_factor = session.developed_driver_factor.bi.to_frame('BI')
    bi_factor = bi_factor.loc[session.ETBR]
    bi_factor = bi_factor.groupby('policy_id')['BI'].apply(
        lambda x: x.sort_values(ascending = False))
    drv_rank = bi_factor.groupby('policy_id').cumcount() + 1
    _, veh_num = session.vehicle_count.align(veh_num)
    rated = drv_rank <= veh_num
    return rated
def get_rated_youthful_driver_count(session):
    tempdf = session.get(['rated', 'age'])
    return (tempdf['rated'] & (tempdf['age'] < 21)).groupby('policy_id').sum()
def get_luxury_vehicle_on_policy(session):
    return session.is_luxury_vehicle.groupby('policy_id').any()
def get_vehicle_count(session):
    return session.vehicles.groupby('policy_id').size()
def get_multi_car(session):
    return session.vehicle_count > 1
def get_pip_discount(session):
    tempdf = session.get(['ETBR_driver_count', 'model_year'])
    bi_factor = session.vehicle_symbol_factor[['BI']]
    tempdf = tempdf.merge(
        bi_factor, left_index = True, right_index = True, how = 'inner')
    tempdf = \
        tempdf.sort_values(by = ['model_year', 'BI'], ascending = [True, True])
    pip_discount = \
        session.vehicles.index.isin(tempdf.groupby('policy_id').first().index)
    return pip_discount


dict_eval_fx = {
    'clean_driver': get_clean_driver,
    'ETBR': get_ETBR,
    'pni_age': get_pni_age,
    'pni_marital_status': get_pni_marital_status,
    'pni_gender': get_pni_gender,
    'pni_youthful': get_pni_youthful,
    'max_driver_age': get_max_driver_age,
    'household_member_count': get_household_member_count,
    'ETBR_driver_count': get_ETBR_driver_count,
    'multiple_ETBR_drivers': get_multiple_ETBR_drivers,
    'rated_driver_count': get_rated_driver_count,
    'youthful_driver_count': get_youthful_driver_count,
    'rated': get_rated,
    'rated_youthful_driver"count': get_rated_youthful_driver_count,
    'luxury_vehicle_on_policy': get_luxury_vehicle_on_policy,
    'vehicle_count': get_vehicle_count,
    'multi_car': get_multi_car,
    'pip_discount': get_pip_discount
}

dict_intermediate_rating_steps = \
    {k: RatingStep(v) for k, v in dict_eval_fx.items()}

nj06.register(**dict_intermediate_rating_steps)

list(nj06.make_dag().static_order())

