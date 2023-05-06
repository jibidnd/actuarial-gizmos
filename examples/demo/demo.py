import pandas as pd
from gzmo.rating.rating_plan import RatingPlan

rating_plan = RatingPlan.from_excel(
    r'examples/demo/demo_rate_manual.xlsx'
    )

for table_name, table in rating_plan.items():
    print((
        f'Table {table_name} '
        + f'has inputs {table.inputs} '
        + f'and outputs {table.outputs}.\n'
    ))


print(rating_plan['credit_tier_placement'])

rating_plan['credit_tier_placement'].evaluate({'credit_score': 795, 'pni_age': 25})

rating_plan

from gzmo.rating.rating_plan import InterpolatedRatingTable


interpolated = InterpolatedRatingTable.from_rating_table(
    rating_plan.amount_of_insurance_factor
    )
print(interpolated.head())
print(interpolated.evaluate(87500))

from gzmo.rating.rating_plan import RatingStep

def get_daily_base_rate(session):
    fixed_premium = (
        (
            session.total_premium
            * session.fixed_portion
        ).round(2)
        + session.fixed_expense
    )
    daily_base_rate = fixed_premium / 182.5
    daily_base_rate = daily_base_rate.clip(0.01)
    return daily_base_rate

rating_plan.register(daily_base_rate = RatingStep(get_daily_base_rate))

print(rating_plan['daily_base_rate'].inputs)

# Order of rating tables from the inputs
print('Order of rating tables from inputs:')
print(rating_plan.keys())

# Recall that credit_tier_factor takes credit tier as an input,
#   which is an output of credit_tier_placement.
# When executed, the rate plan automatically makes a dag,
#   placing crediter_tier_factor after credit_tier_placement
dag = rating_plan.make_dag()
print(list(dag.static_order()))

# session = rating_plan.rate(policies, parallel = True)

# example of parallel processing

rating_plan.pop('daily_base_rate')

import time
from gzmo.rating.utils import make_random_market_basket

num_samples = 100000
random_mb = make_random_market_basket(rating_plan, num_samples)

t0 = time.perf_counter()
session = rating_plan.rate(random_mb)
t1 = time.perf_counter()

# for name, item in session.items():
#     print(name)
#     print(item)

print(f'Running {num_samples:,.0f} records took {t1-t0:.0f} seconds.')


class Company_Base_Rating_Plan(RatingPlan):
    def __init__(self):
        super().__init__()
        self.read_excel(...)
    
    @staticmethod
    def get_max_driver_age(session):
        return session.drivers.groupby('policy_id').max()

    @staticmethod
    def calculate_vehicle_age(session):
        return session.effective_year - session.vehicles.model_year


class Company_Revised_Rating_Plan(Company_Base_Rating_Plan):

    # override a method to cap the vehicle age at 0 minimum
    @staticmethod
    def calculate_vehicle_age(session):
        return max(0, session.effective_year - session.vehicles.model_year)

from gzmo.rating.utils import make_random_market_basket

random_mb = make_random_market_basket(rating_plan, 1000)
print(random_mb.head())

# import numpy as np
from gzmo.rating.utils import make_market_basket

num_policies = 1000
dict_policies = {
    # Pass 'SERIAL' for an auto-incrementing id
    'policy_id': 'SERIAL',
    # Pass an iterable for uniform choices
    'policy_classification': ['I', 'R', 'X'],
    # Pass a tuple for uniform range
    'advance_shop_days': (1, 60),
    # Or pass a series for any custom defintions
    'credit_score': np.where(
        np.random.random_sample(num_policies) < 0.1,
        np.random.randint(0, 2, num_policies),
        np.rint(np.random.normal(750, 100, num_policies))
    )
}
# Simply pass the specificaiton and the number of desired records:
df_policies = make_market_basket(dict_policies, num_policies)

print(df_policies)

# Drvers
# =============================================================================
# Each policy will get anywhere from 1 to 4 drivers
num_drivers_per_policy = np.random.randint(1, 5, num_policies)
driver_policy_ids = df_policies['policy_id'].repeat(num_drivers_per_policy)
driver_ids = driver_policy_ids.groupby(driver_policy_ids).cumcount() + 1
num_drivers = len(driver_policy_ids)

dict_drivers = {
    'policy_id': driver_policy_ids,
    'driver_id': driver_ids,
    'is_primary': driver_ids == 1,
    'age': np.rint(np.random.normal(35, 30, num_drivers)).clip(16, 120),
    'gender': 'MF',
    'marital_status': 'SM',
    'months_experienced': (0, 60)
}
# Create the driver market basket
df_drivers = make_market_basket(dict_drivers, num_drivers)

# Each policy will get anywhere from 1 to 4 vehicles
num_vehicles_per_policy = np.random.randint(1, 5, num_policies)
vehicle_policy_ids = df_policies['policy_id'].repeat(num_vehicles_per_policy)
vehicle_ids = vehicle_policy_ids.groupby(vehicle_policy_ids).cumcount() + 1
num_vehicles = len(vehicle_policy_ids)



dict_vehicles = {
    'policy_id': vehicle_policy_ids,
    'vehicle_id': vehicle_ids,
    'model_year': (1960, 2022),
    'recovery_device_type': [
        'ALARM_ONLY',
        'NON_PASSIVE_ALARM',
        'PASSIVE_ALARM',
        'TRACKING_DEVICE',
        'PASSIVE_ALARM_TRACKING_DEVICE',
        'NONE'
    ],
    'COMP_deductible': [100, 150, 250, 500, 750, 1000, 1500, 2000, 9999],
    'COLL_deductible': [100, 150, 250, 500, 750, 1000, 1500, 2000, 9999]
}
df_vehicles = make_market_basket(dict_vehicles, num_vehicles)

print(df_policies)
print(df_drivers)
print(df_vehicles)



from gzmo.base import SearchableDict

market_basket = SearchableDict(
    policies = df_policies,
    drivers = df_drivers,
    vehicles = df_vehicles
)

# accessing whole tables
print(market_basket.drivers)

# accessing an attribute
print(market_basket.gender)

# accessing multiple attributes
print(market_basket[['age', 'gender', 'marital_status']])

# accessing multiple attributes from MULTIPLE tables!
# note the new indices
print(market_basket[['policy_classification', 'model_year']])