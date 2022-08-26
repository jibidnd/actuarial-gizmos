from distutils.archive_util import make_archive
import pandas as pd
import numpy as np
import string

from gzmo.rating.helpers import make_market_basket

# read in rating plan for list of possible occupation, zip codes, etc
df_rating_plan = \
    pd.read_excel(r'examples/Metromile/NJ06.xlsx', sheet_name = None)

# dict_test = {
#     'policy_id': 'SERIAL',
#     'policy_type': 'SB',
#     'advance_shop_days': (1, 60),
#     'eft': ['Y', 'N'],
#     'default': 'X',
#     'num_policies': 1
# }

# df = make_market_basket(dict_test, 5)

# Policies
# =============================================================================
num_policies = 1000

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
    'zip_code': \
        df_rating_plan['territory_assignment']._zipcode.unique(),
    'homeowner': 'YN',
    'homeowner_at_init': 'YN',
    'vehicle_count_at_init': 'YN',
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
    'BI_limit': ['10/10', '15/30', '25/50', '50/100', '100/300', '250/500'],
    'limitation_on_lawsuits': ['LIMITED', 'FULL'],
    'PD_limit': [5, 10, 25, 50, 100],
    'PIP_limit': [15, 50, 75, 150, 250, 'NONE'],
    'PIP_deductible': [250, 500, 1000, 2000, 2500, 'NONE'],
    'coverage_group': ['PRIMARY', 'SECONDARY', 'NONE'],
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
num_drivers = len(driver_policy_ids)

dict_drivers = {
    'policy_id': df_policies['policy_id'].repeat(num_drivers_per_policy),
    'driver_id': 'SERIAL',
    'age': np.rint(np.random.normal(35, 30, num_drivers)).clip(16, 120),
    'gender': 'MF',
    'marital_status': 'SM',
    'months_experienced': (0, 60),
    'defensive_driver': 'YN',
    'employment_status': \
        df_rating_plan['occupation_group']._employment_status.unique(),
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
num_vehicles = len(vehicle_policy_ids)

# year, make, model, style need to be consistent with each other
# i.e. you can't have a Toyota Civic!
df_vehicle_sample = df_rating_plan['vehicle_symbol_factor'][
    ['_model_year_left', '_model_year_right', '_make', '_model', '_style']
    ].sample(num_vehicles)
df_vehicle_sample['model_year'] = np.random.randint(
    df_vehicle_sample['_model_year_left'],
    df_vehicle_sample['_model_year_right'] + 1,
    num_vehicles
)


dict_vehicles = {
    'policy_id': vehicle_policy_ids,
    'vehicle_id': 'SERIAL',
    'model_year': df_vehicle_sample['model_year'],
    'make': df_vehicle_sample['_make'],
    'model': df_vehicle_sample['_model'],
    'style': df_vehicle_sample['_style'],
    'vehicle_age': (2022 - df_vehicle_sample['model_year']).clip(0),
    'vehicle_risk_group_at_init': 'YN',
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
    'LOAN_limit': ['Y', 'NONE'],
    'RENT_limit': ['30/900', '40/1200', '50/1500', 'NONE'],
    'ROAD_limit': ['Y', 'NONE'],
    'ACPE_limit': (0, 4000),
    'EXTMED_limit': [0, 1000, 10000],
    'ESSSRV_limit': ['12/4380', '12/8760', '20/14600', '0/0'],
    'DEATH_limit': ['BASE', 10, 'NONE'],
    'FUNERAL_limit': [1, 2, 'NONE'],
    'funeral_coverage_group': df_rating_plan['IL_limit_factor']._IL_limit.unique(),
    'IL_limit': ['BLANK', 'YOU_AND_SPOUSE', 'YOU_AND_SPOUSE_AND_RESIDENT_RELATIVES'],
    'COMP_deductible': df_rating_plan['COMP_deductible_factor']._COMP_deductible.unique(),
    'COLL_deductible': [100, 150, 250, 500, 750, 1000, 1500, 2000, 9999]
}
df_vehicles = make_market_basket(dict_vehicles, num_vehicles)