import pandas as pd

from gzmo.rating.rating_plan import RatingPlan, RatingStep

class NJ_Base_RatingPlan(RatingPlan):
    def __init__(self):
        super().__init__()

        # Define a name for all function outputs
        dict_eval_fx = {
            'clean_driver': self.get_clean_driver,
            'ETBR': self.get_ETBR,
            'pni_age': self.get_pni_age,
            'pni_marital_status': self.get_pni_marital_status,
            'pni_gender': self.get_pni_gender,
            'pni_youthful': self.get_pni_youthful,
            'max_driver_age': self.get_max_driver_age,
            'household_member_count': self.get_household_member_count,
            'ETBR_driver_count': self.get_ETBR_driver_count,
            'multiple_ETBR_drivers': self.get_multiple_ETBR_drivers,
            'driving_record_points_factor': self.get_driver_record_points_factor,
            'driver_age_point_factor': self.get_driver_age_point_factor,
            'developed_driver_factor': self.get_developed_driver_factor,
            'rated': self.get_rated,
            'household_risk_factor': self.get_household_risk_factor,
            'rated_driver_count': self.get_rated_driver_count,
            'youthful_driver_count': self.get_youthful_driver_count,
            'rated_youthful_driver_count': self.get_rated_youthful_driver_count,
            'policy_education_occupation_factor': self.get_policy_education_occupation_factor,
            'policy_defensive_driver_factor': self.get_policy_defensive_driver_factor,
            'luxury_vehicle_on_policy': self.get_luxury_vehicle_on_policy,
            'vehicle_count': self.get_vehicle_count,
            'multi_car': self.get_multi_car,
            'pip_discount': self.get_pip_discount,
            'limit_factor': self.get_limit_factor,
            'deductible_factor': self.get_deductible_factor,
            'fixed_cost': self.get_fixed_cost,
            'total_premium': self.get_total_premium,
            'daily_base_rate': self.get_daily_base_rate,
            'per_mile_rate': self.get_per_mile_rate
        }

        # put them in a dict for import
        dict_intermediate_rating_steps = \
            {k: RatingStep(v) for k, v in dict_eval_fx.items()}

        self.register(**dict_intermediate_rating_steps)
    

    @staticmethod
    def get_clean_driver(session):
        # Rule D03
        #   Clean Driver Classification –
        #       Drivers with zero BI/PD points are classified as “Clean” drivers.
        points = \
            session \
                .get(['BI_points', 'PD_points']) \
                .sum(axis = 1)
        return (points == 0).replace({True: 'Y', False: 'N'})

    @staticmethod
    def get_ETBR(session):
        return ~session.drivers['list_only']

    @staticmethod
    def get_pni_age(session):
        pni_age = session.drivers.query('is_primary')['age']
        # drop driver_id index
        pni_age = pni_age.reset_index('driver_id', drop = True)
        return pni_age

    @staticmethod
    def get_pni_marital_status(session):
        pni_marital_status = \
            session.drivers.query('is_primary')['marital_status']
        # drop driver_id index
        pni_marital_status = \
            pni_marital_status.reset_index('driver_id', drop = True)
        return pni_marital_status

    @staticmethod
    def get_pni_gender(session):
        pni_gender = \
            session.drivers.query('is_primary')['gender']
        # drop driver_id index
        pni_gender = \
            pni_gender.reset_index('driver_id', drop = True)
        return pni_gender

    @staticmethod
    def get_pni_youthful(session):
        # Rule D01: The term “youthful” refers to drivers under the age of 21.
        youthful_age = 21
        pni_youthful = (session.pni_age < youthful_age).replace({True: 'Y', False: 'N'})
        return pni_youthful

    @staticmethod
    def get_max_driver_age(session):
        return session.drivers.groupby('policy_id')['age'].max()

    @staticmethod
    def get_household_member_count(session):
        # Rule D01
        #   Under certain circumstances, the applicant may designate specific
        #   household members as "list only" drivers...
        return session.drivers.groupby('policy_id').size()

    @staticmethod
    def get_ETBR_driver_count(session):
        # Rule D01:
        #   The term "eligible to be rated driver" refers to resident relatives
        #   of legal driving age and drivers of insured vehicles other than those
        #   designated as list only
        return session.ETBR.groupby('policy_id').size()

    @staticmethod
    def get_multiple_ETBR_drivers(session):
        return (session.ETBR_driver_count > 1).replace({True: 'Y', False: 'N'})

    @staticmethod
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
        return session.rated.groupby('policy_id').sum()

    @staticmethod
    def get_youthful_driver_count(session):
        # Rule P29
        #   The “Youthful Driver Count” counts eligible to be rated,
        #   excluded and list only “youthful” drivers (rule D01).
        drivers = session.drivers
        drivers.loc[(session.ETBR) & (drivers.age <= 21)]
        return \
            drivers.groupby('policy_id').size()

    @staticmethod
    def get_driver_record_points_factor(session):
        driver_record_points_factor = (
            session.driving_points_factor_BI
            .join(session.driving_points_factor_PD)
            .join(session.driving_points_factor_COMP)
            .join(session.driving_points_factor_COLL)
            .join(session.driving_points_factor_PIP)
            .join(session.driving_points_factor_UMUIM)
            .join(session.driving_points_factor_UMPD)
            .join(session.driving_points_factor_ROAD)
        )
        return driver_record_points_factor

    @staticmethod
    def get_driver_age_point_factor(session):
        driver_record_points_factor = (
            session.driver_age_point_factor_BI          # BI, PD
            .join(session.driver_age_point_factor_COMP) # COMP, LOAN
            .join(session.driver_age_point_factor_COLL) # COLL, UMPD, RENT, ROAD
            .join(session.driver_age_point_factor_PIP)  # PIP, UMUIM
        )
        return driver_record_points_factor

    @staticmethod
    def get_developed_driver_factor(session):
        developed_driver_factor = \
            (
                session.driver_classification_factor * \
                session.experienced_driver_factor + \
                session.driving_record_points_factor
             ) * \
            session.household_member_factor * \
            session.driver_age_point_factor * \
            session.credit_clean_driver_factor
        return developed_driver_factor

    @staticmethod
    def get_rated(session):
        bi_factor = session.developed_driver_factor.BI
        # non-ETBR drivers aren't eligible to get rated, duh.
        bi_factor = bi_factor.where(session.ETBR, 0)
        drv_rank = (
            bi_factor
            .groupby('policy_id')
            .rank(method = 'first', ascending = False)
            .astype(int)
        )
        veh_num, _ = session.vehicle_count.align(drv_rank)
        rated = drv_rank <= veh_num
        return rated
    
    @staticmethod
    def get_household_risk_factor(session):
        rated_factors = session.developed_driver_factor.loc[session.rated]
        rated_average = rated_factors.groupby('policy_id').mean()
        return rated_average

    @staticmethod
    def get_rated_youthful_driver_count(session):
        tempdf = session.get(['rated', 'age'])
        return (tempdf['rated'] & (tempdf['age'] < 21)).groupby('policy_id').sum()

    @staticmethod
    def get_policy_education_occupation_factor(session):
        # Rule P24:
        #   Education/occupation rating factors will be applied based on the most
        #   favorably ranked pairing of education and occupation for the
        #   eligible to be rated primary named insured or other eligible to be rated
        #   married drivers if the primary named insured is married.

        participating = (
            session.drivers.is_primary |
            (
                (session.pni_marital_status == 'M') &
                (session.marital_status == 'M')
            )
        )
        bi_factor = session.education_occupation_factor['BI']
        # non-participating folks get a high factors so they don't get ranked
        bi_factor = bi_factor.where(participating, 999)
        ranks = (
            bi_factor
            .groupby('policy_id')
            .rank(method = 'first', ascending = True)
        )
        selected = (ranks==1)
        policy_education_occupation_factor = \
            session.education_occupation_factor.loc[selected]
        # drop driver_id
        policy_education_occupation_factor = (
            policy_education_occupation_factor
            .reset_index('driver_id', drop = True)
        )
        return policy_education_occupation_factor

    @staticmethod
    def get_policy_defensive_driver_factor(session):
        # Rule D04 references "automobiles" assigned to...
        # but there is no vehicle assignment in NJ.
        # We'll just apply the discount to all vehicles
        # on the policy if any driver has it
        return session.defensive_driver_factor.groupby('policy_id').min()


    @staticmethod
    def get_luxury_vehicle_on_policy(session):
        any_luxury = session.is_luxury_vehicle.groupby('policy_id').any()
        return any_luxury.replace({True: 'Y', False: 'N'})

    @staticmethod
    def get_vehicle_count(session):
        return session.vehicles.groupby('policy_id').size()

    @staticmethod
    def get_multi_car(session):
        return (session.vehicle_count > 1).replace({True: 'Y', False: 'N'})

    @staticmethod
    def get_pip_discount(session):
        tempdf = \
            session.get(['ETBR_driver_count', 'vehicle_count', 'model_year'])
        bi_factor = session.vehicle_symbol_factor[['BI']].sort_index()
        tempdf = tempdf.merge(
            bi_factor, left_index = True, right_index = True, how = 'inner')
        tempdf = tempdf.sort_values(
            by = ['model_year', 'BI'],
            ascending = [True, True]
            )
        full_rate_rank = tempdf.groupby(['policy_id']).cumcount()
        has_pip_discount = \
            (tempdf['ETBR_driver_count'] == 1) & \
            (tempdf['vehicle_count'] > 1) & \
            (full_rate_rank > 0)
        has_pip_discount = has_pip_discount.sort_index()
        has_pip_discount = has_pip_discount.replace({True: 'Y', False: 'N'})

        return has_pip_discount
    
    @staticmethod
    def get_limit_factor(session):
        limit_factor = (
            session.BI_limit_factor
            .join(session.PD_limit_factor)
            .join(session.UMUIM_limit_factor)
            .join(session.UMPD_limit_factor)
            .join(session.LOAN_limit_factor)
            .join(session.RENT_limit_factor)
            .join(session.PIP_limit_factor)
            .join(session.ACPE_limit_factor)
            .join(session.ROAD_limit_factor)
            .join(session.EXTMED_limit_factor)
        )
        return limit_factor
    
    @staticmethod
    def get_deductible_factor(session):
        deductible_factor = (
            session.COMP_deductible_factor
            .join(session.COLL_deductible_factor)
        )
        return deductible_factor

    @staticmethod
    def get_fixed_cost(session):
        fixed_cost = (
            session.base_rate[['ACQ']]
            * session.acq_full_coverage_factor
            * session.acq_homeowner_factor
            * session.acq_online_quote_factor
            * session.acq_prior_insurance_factor
            * session.acq_vehicle_count_factor
            * session.acq_multi_policy_factor
            / session.vehicle_count
        )
        return fixed_cost

    @staticmethod
    def get_total_premium(session):
        total_premium = (
            (
                session.household_risk_factor
                * session.base_rate
                * session.policy_risk_factor
                * session.credit_tier_factor
                * session.credit_driver_count_factor
                * session.policy_education_occupation_factor
                * session.full_coverage_factor
                * session.household_structure_factor
                * session.luxury_vehicle_factor
                * session.advance_quote_factor
                * session.limit_factor
                * session.deductible_factor
                * session.vehicle_age_factor
                * session.vehicle_symbol_factor
                * session.garaging_location_factor
                * session.multi_car_homeowner_factor
                * session.three_year_safe_driving_factor
                * session.five_year_claim_free_factor
                * session.payment_method_factor
                * session.online_quote_discount_factor
                * session.paperless_discount_factor
                * session.continuous_insurance_factor
                * session.multi_policy_discount_factor
                * session.pip_discount_factor
                * session.business_use_factor
                * session.antitheft_device_factor
                * session.policy_defensive_driver_factor
                * session.bad_debt_factor
            ).drop('ACQ', axis = 1)
            + session.fixed_cost
        )
        return total_premium


    @staticmethod
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
    
    @staticmethod
    def get_per_mile_rate(session):
        variable_premium = (
            session.total_premium
            * session.variable_portion
        ).round(2)
        per_mile_rate = variable_premium / session.base_miles
        per_mile_rate = per_mile_rate.clip(0.001)
        return per_mile_rate
