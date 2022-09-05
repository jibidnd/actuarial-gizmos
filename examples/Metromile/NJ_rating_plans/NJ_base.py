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
            'luxury_vehicle_on_policy': self.get_luxury_vehicle_on_policy,
            'vehicle_count': self.get_vehicle_count,
            'multi_car': self.get_multi_car,
            'pip_discount': self.get_pip_discount
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
        return session.drivers.query('is_primary')['age']

    @staticmethod
    def get_pni_marital_status(session):
        return session.drivers.query('is_primary')['marital_status']

    @staticmethod
    def get_pni_gender(session):
        return session.drivers.query('is_primary')['gender']

    @staticmethod
    def get_pni_youthful(session):
        # Rule D01: The term “youthful” refers to drivers under the age of 21.
        youthful_age = 21
        return (session.pni_age < youthful_age).replace({True: 'Y', False: 'N'})
        # return (session.drivers.query('is_primary')['age'] < youthful_age).replace({True: 'Y', False: 'N'})

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
        return session.get(['ETBR_driver_count', 'vehicle_count']).min()

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
        driver_record_points_factor = pd.concat([
            session.driving_points_factor_BI,
            session.driving_points_factor_PD,
            session.driving_points_factor_COMP,
            session.driving_points_factor_COLL,
            session.driving_points_factor_PIP,
            session.driving_points_factor_UMUIM,
            session.driving_points_factor_UMPD,
            session.driving_points_factor_ROAD
        ], axis = 1)
        return driver_record_points_factor

    @staticmethod
    def get_driver_age_point_factor(session):
        driver_record_points_factor = pd.concat([
            session.driver_age_point_factor_BI,     # BI, PD
            session.driver_age_point_factor_COMP,   # COMP, LOAN
            session.driver_age_point_factor_COLL,   # COLL, UMPD, RENT, ROAD
            session.driver_age_point_factor_PIP     # PIP, UMUIM
        ], axis = 1)
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
        bi_factor = session.developed_driver_factor.bi.to_frame('BI')
        bi_factor = bi_factor.loc[session.ETBR]
        bi_factor = bi_factor.groupby('policy_id')['BI'].apply(
            lambda x: x.sort_values(ascending = False))
        drv_rank = bi_factor.groupby('policy_id').cumcount() + 1
        _, veh_num = session.vehicle_count.align(drv_rank)
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