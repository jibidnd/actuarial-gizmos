import pandas as pd
import numpy as np
import string
from gzmo.base import FancyDict, SearchableDict

from gzmo.rating.rating_plan import RatingPlan
from gzmo.rating.utils import make_market_basket, make_random_market_basket

# read in rating plan for list of possible occupation, zip codes, etc
dict_ratingplan = \
    pd.read_excel(r'examples/Metromile/NJ06.xlsx', sheet_name = None)
# initialize an empty rating plan
nj06 = RatingPlan.from_unprocessed_dataframes(dict_ratingplan)


# Policies
# =============================================================================
num_policies = 1000
# fpb_cov = np.random.random(num_policies)
il_limit = np.random.choice(
    dict_ratingplan['IL_limit_factor']._IL_limit.unique(),
    num_policies
    )
funeral_limit = np.where(
    np.isin(il_limit, ['100/5200', 'NONE']),
    np.random.choice([1, 'NONE'], num_policies),
    2
    )
fpb_coverage_group = np.where(
    np.isin(il_limit, ['100/5200', 'NONE']),
    'BLANK',
    np.random.choice(
        [
            'YOU_AND_SPOUSE',
            'YOU_AND_SPOUSE_AND_RESIDENT_RELATIVES'
        ],
        num_policies)
    )

dict_policies = {
    'policy_id': 'SERIAL',
    'policy_type': 'SB',
    'policy_classification': 'IRX',
    'quote_type': 'IP',
    'advance_shop_days': (1, 60),
    'eft': 'YN',
    'automatic_card_payment': 'YN',
    'online_quote': 'YN',
    'paperless': 'YN',
    'credit_score': np.where(
        np.random.random_sample(num_policies) < 0.1,
        np.random.randint(0, 2, num_policies),
        np.rint(np.random.normal(750, 100, num_policies))
    ),
    # roughly the list of zips in NJ
    'zipcode': \
        dict_ratingplan['territory_assignment']._zipcode.unique(),
    'homeowner': 'YN',
    'homeowner_at_init': 'YN',
    'vehicle_count_at_init': (1, 4),
    'tenure': np.random.randint(0, 120),
    'prior_insurance_code': 'ABC',
    'prior_insurance_level': 'ABCDE',
    'prior_bi_code': '012345NX',
    # the list of possible tiers from Metromile rate manual
    'tier': np.random.choice(
        [f'{x}{y}' for x in np.arange(1, 8) for y in string.ascii_uppercase] +
        [f'8{y}' for y in string.ascii_uppercase[:18]],
        num_policies
    ),
    'continuous_insurance_discount_level': [
        'SILVER', 'SILVER_SELECT', 'GOLD', 'GOLD_SELECT', 
        'PLATINUM_1', 'PLATINUM_1_SELECT', 'PLATINUM_2_SELECT', 
        'DIAMOND', 'DIAMOND_SELECT', 'NONE'
        ],
    'silver_continuous_insurance_discount_at_init': 'YN',
    'gold_continuous_insurance_discount_at_init': 'YN',
    'nb_five_year_accident_free_discount': 'YN',
    'five_year_claim_free_discount': 'YN',
    'three_year_safe_driving_discount': 'YN',
    'BI_limit': ['15/30', '25/50', '50/100', '100/300', '250/500'],
    'limitation_on_lawsuits': ['LIMITED', 'FULL'],
    'PD_limit': [5, 10, 25, 50, 100],
    'PIP_limit': [15, 50, 75, 150, 250, 'NONE'],
    'PIP_deductible': [250, 500, 1000, 2000, 2500],
    'PIP_coverage_group': ['PRIMARY', 'SECONDARY'],
    'fpb_coverage_group': fpb_coverage_group,
    'EXTMED_limit': [0, 1000, 10000],
    'ESSSRV_limit': ['12/4380', '12/8760', '20/14600', '0/0'],
    'DEATH_limit': ['BASE', 10, 'NONE'],
    'FUNERAL_limit': funeral_limit,
    'IL_limit': il_limit,
    'UMUIM_limit': ['15/30', '25/50', '50/100', '100/300', '250/500'],
    'UMPD_limit': [5, 10, 25, 50, 100]
}

# Create the policy market basket
df_policies = make_market_basket(dict_policies, num_policies)

# Drvers
# =============================================================================
# Each policy will get anywhere from 1 to 4 drivers
num_drivers_per_policy = np.random.randint(1, 5, num_policies)
driver_policy_ids = df_policies['policy_id'].repeat(num_drivers_per_policy)
driver_ids = driver_policy_ids.groupby(driver_policy_ids).cumcount() + 1
num_drivers = len(driver_policy_ids)

# valid education/occupation combinations
df_edu_occ_choices = dict_ratingplan['occupation_group'][
    ['_employment_status', '_occupation_description']
    ].sample(num_drivers, replace = True)

dict_drivers = {
    'policy_id': driver_policy_ids,
    'driver_id': driver_ids,
    'is_primary': driver_ids == 1,
    'list_only': \
        (np.random.random_sample(num_drivers) < 0.1) & \
        (driver_ids != 1),
    'age': np.rint(np.random.normal(35, 30, num_drivers)).clip(16, 120),
    'gender': 'MF',
    'marital_status': 'SM',
    'months_experienced': (0, 60),
    'defensive_driver': 'YN',
    'employment_status': df_edu_occ_choices['_employment_status'],
    'occupation_description': df_edu_occ_choices['_occupation_description'],
    'education_level': '1234567X',
    'aaf_count': \
        np.random.choice(5, num_drivers, p = [0.85, 0.1, 0.025, 0.02, 0.005]),
    'dwi_count': \
        np.random.choice(5, num_drivers, p = [0.85, 0.1, 0.025, 0.02, 0.005]),
    'ind_count': \
        np.random.choice(5, num_drivers, p = [0.85, 0.1, 0.025, 0.02, 0.005]),
    'maj_count': \
        np.random.choice(5, num_drivers, p = [0.85, 0.1, 0.025, 0.02, 0.005]),
    'min_count': \
        np.random.choice(5, num_drivers, p = [0.85, 0.1, 0.025, 0.02, 0.005]),
    'naf_count': \
        np.random.choice(5, num_drivers, p = [0.85, 0.1, 0.025, 0.02, 0.005]),
    'spd_count': \
        np.random.choice(5, num_drivers, p = [0.85, 0.1, 0.025, 0.02, 0.005])
}
# Create the driver market basket
df_drivers = make_market_basket(dict_drivers, num_drivers)

# Vehicles
# =============================================================================
# Each policy will get anywhere from 1 to 4 vehicles
num_vehicles_per_policy = np.random.randint(1, 5, num_policies)
vehicle_policy_ids = df_policies['policy_id'].repeat(num_vehicles_per_policy)
vehicle_ids = vehicle_policy_ids.groupby(vehicle_policy_ids).cumcount() + 1
num_vehicles = len(vehicle_policy_ids)

# year, make, model, style need to be consistent with each other
# i.e. you can't have a Toyota Civic!
df_vehicle_sample = dict_ratingplan['vehicle_symbol_factor'][
    ['_model_year_left', '_model_year_right', '_make', '_model', '_style']
    ].sample(num_vehicles)
df_vehicle_sample['model_year'] = np.random.randint(
    df_vehicle_sample['_model_year_left'],
    df_vehicle_sample['_model_year_right'] + 1,
    num_vehicles
)


dict_vehicles = {
    'policy_id': vehicle_policy_ids,
    'vehicle_id': vehicle_ids,
    'model_year': df_vehicle_sample['model_year'],
    'make': df_vehicle_sample['_make'],
    'model': df_vehicle_sample['_model'],
    'style': df_vehicle_sample['_style'],
    'vehicle_age': (2022 - df_vehicle_sample['model_year']).clip(0),
    'vehicle_risk_group_at_init': ['A1', 'B1', 'C1', 'D1', 'E1'],
    'recovery_device_type': [
        'ALARM_ONLY',
        'NON_PASSIVE_ALARM',
        'PASSIVE_ALARM',
        'TRACKING_DEVICE',
        'PASSIVE_ALARM_TRACKING_DEVICE',
        'NONE'
    ],
    'vehicle_clean_status': 'YNX',
    'full_coverage_on_vehicle': 'YN',
    'business_use': 'N',
    'full_coverage_code': 'ASN',
    'full_coverage_at_init': 'ASN',
    'LOAN_limit': ['Y', 'NONE'],
    'RENT_limit': ['30/900', '40/1200', '50/1500', 'NONE'],
    'ROAD_limit': ['Y', 'NONE'],
    'ACPE_limit': (0, 4000),
    'COMP_deductible': dict_ratingplan['COMP_deductible_factor']._COMP_deductible.unique(),
    'COLL_deductible': [100, 150, 250, 500, 750, 1000, 1500, 2000, 9999]
}
df_vehicles = make_market_basket(dict_vehicles, num_vehicles)

# create the market basket with 3 dataframes
nj06_market_basket = SearchableDict(
    policies = df_policies,
    drivers = df_drivers,
    vehicles = df_vehicles
)

# OR simply:
# df = make_random_market_basket(nj06, 1000)
