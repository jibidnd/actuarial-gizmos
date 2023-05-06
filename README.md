# actuarial-gizmos

An easy-to-use yet flexible rating engine *builder*!

## What is it?
**actuarial-gizmos** is a Python package that automates much of the rating engine building process. Its one main objective is:
- Easy to use

You can build a rater in as little as 3 lines of code:

    >>> from gzmo.rating.rating_plan import RatingPlan
    >>> rating_plan = RatingPlan.from_excel(r'examples/demo/demo_rate_manual.xlsx')
    >>> session = rating_plan.rate(market_basket)

## Features

<details>
<summary><b>Rating Plans from tables or python functions</b></summary>

All you need is:
    
    >>> from gzmo.rating.rating_plan import RatingPlan
    >>> rating_plan = RatingPlan.from_excel(r'examples/demo/demo_rate_manual.xlsx')

And the package will do the rest:

*   <details>
    <summary><b>Automatically infers inputs and outputs</b></summary>
    
    The rater automatically infers inputs and outputs.
    This works on both tables and [custom python functions](#customfx)!

        >>> for table_name, table in rating_plan.items():
        ...     print((
        ...         f'Table {table_name} '
        ...         + f'has inputs {table.inputs} '
        ...         + f'and outputs {table.outputs}.\n'
        ...     ))
        ... 
        Table base_rates has inputs [] and outputs ['BI', 'PD'].

        Table credit_tier_factor has inputs ['prior_insurance_code', 'credit_tier'] and outputs ['BI', 'PD'].

        Table amount_of_insurance_factor has inputs ['amount_of_insurance'] and outputs ['BI', 'PD'].

        Table credit_tier_placement has inputs ['credit_score', 'pni_age'] and outputs ['credit_tier'].
    
    </details>

*   <details>
    <summary><b>Suports intervals and wildcards as table inputs</b></summary>
    
    The package supports interval inputs (indicated by specifying the upper and lower ends of the intervals--see example rate plans) as well as wild cards (indicated by a `*`).

        >>> print(rating_plan['credit_tier_placement'])
                                credit_tier
        credit_score    pni_age                 
        [-inf, inf]     [-inf, inf]           I1
        [790.0, 900.0]  [0.0, 53.0]           A1
                        [54.0, 60.0]          A1
                        [61.0, 80.0]          A1
        [765.0, 789.0]  [0.0, 53.0]           B1
        ...                                 ...
        [1.0, 1.0]      [54.0, 60.0]          T4
                        [61.0, 80.0]          T5
        [0.0, 0.0]      [0.0, 53.0]           X3
                        [54.0, 60.0]          X4
                        [61.0, 80.0]          X5

        [61 rows x 1 columns]

        >>> rating_plan['credit_tier_placement'].evaluate({'credit_score': 795, 'pni_age': 25})
        {'credit_tier': 'I1'}


    </details>

*   <details>
    <summary><b>Supports interpolation of numeric inputs</b></summary>

    Interpolating a numeric input is a one-liner:

        >>> interpolated = InterpolatedRatingTable.from_rating_table(
        ...     rating_plan.amount_of_insurance_factor
        ...     )
        >>> 
        >>> print(interpolated.head())
                                BI    PD
        amount_of_insurance            
        80000                   0.56  0.56
        95000                   0.63  0.63
        110000                  0.69  0.69
        125000                  0.75  0.75
        140000                  0.81  0.81
        >>> print(interpolated.evaluate(87500))
        {'BI': 0.595, 'PD': 0.595}

    </details>

*   <details>
    <summary><b>Accepts custom Python functions as rating steps</b></summary>
    
    Have logic that doesn't fit well as a rating table? If you can write it in Python, the rater will take it!

        >>> def get_daily_base_rate(session):
        ...     fixed_premium = (
        ...         (
        ...             session.total_premium
        ...             * session.fixed_portion
        ...         ).round(2)
        ...         + session.fixed_expense
        ...     )
        ...     daily_base_rate = fixed_premium / 182.5
        ...     daily_base_rate = daily_base_rate.clip(0.01)
        ...     return daily_base_rate
        ... 
        >>> rating_plan.register(daily_base_rate = RatingStep(get_daily_base_rate))

    <a name="customfx"></a>And it will automatically extract the inputs used in the function!
    
        >>> print(rating_plan['daily_base_rate'].inputs)
        ['fixed_portion', 'total_premium', 'fixed_expense']

    </details>

*   <details>
    <summary><b>Automatically builds dag to sequence rating steps to consider dependencies</b></summary>
    
    The rater will automatically create a dag (directed acyclic graph) to sequence which rating steps to evaluate first. Below is an example of two dependent steps:

        >>> print('Order of rating tables from inputs:')
        Order of rating tables from inputs:
        >>> print(rating_plan.keys())
        dict_keys(['base_rates', 'credit_tier_factor', 'amount_of_insurance_factor', 'credit_tier_placement', 'daily_base_rate'])
        >>> # Recall that credit_tier_factor takes credit tier as an input,
        >>> #   which is an output of credit_tier_placement.
        >>> # When executed, the rate plan automatically makes a dag,
        >>> #   placing crediter_tier_factor after credit_tier_placement
        >>> 
        >>> dag = rating_plan.make_dag()
        >>> print(list(dag.static_order()))
        ['base_rates', 'credit_tier_placement', 'amount_of_insurance_factor', 'daily_base_rate', 'credit_tier_factor']
    </details>

*   <details>
    <summary><b>Runs a portfolio with just one line of code</b></summary>

    It is really just as simple as

        >>> session = rating_plan.rate(market_basket)
    </details>

*   <details>
    <summary><b>It's pretty fast!</b></summary>

    Specifying `parallel = True` in `RatingPlan.rate` will utilize the multiple processors on your machine, and concurrently run rating steps that do not depend on each other.

    Let's see how long it takes to run a simple rate plan on 100,000 records.
    First make a market basket (see [Market Basket Generator](#mbgenerator))

        >>> num_samples = 100000
        >>> random_mb = make_random_market_basket(rating_plan, num_samples)
    
    Now we can test the performance:

        >>> t0 = time.perf_counter()
        >>> session = rating_plan.rate(random_mb)
        >>> t1 = time.perf_counter()
        >>> print(f'Rating {num_samples:,.0f} records took {t1-t0:.0f} seconds.')
        Rating 100,000 records took 90 seconds.

*   <details>
    <summary><b>Allows modular design of rate plans</b></summary>

    Want to have multiple iterations of a rate plan? No problem!
    
    Let's say an initial rate plan was built like the following:

        >>> class Company_Base_Rating_Plan(RatingPlan):
        ...     def __init__(self):
        ...         super().__init__()
        ...         self.read_excel(...)
        ...     @staticmethod
        ...     def get_max_driver_age(session):
        ...         return session.drivers.groupby('policy_id').max()
        ...     @staticmethod
        ...     def calculate_vehicle_age(session):
        ...         return session.effective_year - session.vehicles.model_year
    
    Then down the road, we decide that negative vehicle age is not a good idea. To create a new rate plan, simply inhereit the old one and override methods as needed:

        >>> class Company_Revised_Rating_Plan(Company_Base_Rating_Plan):
        ...     # override a method to cap the vehicle age at 0 minimum
        ...     @staticmethod
        ...     def calculate_vehicle_age(session):
        ...         return max(0, session.effective_year - session.vehicles.model_year)

    </details>

</details>

<details>
<summary><b>Market Basket Generator</b></summary>

Two methods exist for easily creating a market basket:

*   <details>
    <summary><b>Automatically learn from the rating tables</b></summary>

    The function extracts the inputs from all the rating tables in the
        rating plan, and create a market basket with all inputs that are
        needed for the rating plan.
    
    The function will check all tables for input constraints, and will not
        create records with impossible inputs. For example, it will not create
        a record that has both a senior driving discount and a youthful driver
        status.
    <a name="mbgenerator"></a>
    
        >>> from gzmo.rating.utils import make_random_market_basket
        >>> random_mb = make_random_market_basket(rating_plan, 1000)
        
        >>> print(random_mb.head())
        prior_insurance_code  amount_of_insurance  credit_score  pni_age
        0                    A               110000           462       60
        1                    B               185000           764       37
        2                    C               470000           462       57
        3                    C                80000           465       60
        4                    C               260000           425       56

    </details>

*   <details>
    <summary><b>Customly define the each variable</b></summary>

    Alternatively, you can define a more customized market basket, possibly containing multiple tables.
    The following example illustrates the supported syntax:

        >>> from gzmo.rating.utils import make_market_basket
        >>> num_policies = 1000
        >>> dict_policies = {
        ...     # Pass 'SERIAL' for an auto-incrementing id
        ...     'policy_id': 'SERIAL',
        ...     # Pass an iterable for uniform choices
        ...     'policy_classification': ['I', 'R', 'X'],
        ...     # Pass a tuple for uniform range
        ...     'advance_shop_days': (1, 60),
        ...     # Or pass a series for any custom defintions
        ...     'credit_score': np.where(
        ...         np.random.random_sample(num_policies) < 0.1,
        ...         np.random.randint(0, 2, num_policies),
        ...         np.rint(np.random.normal(750, 100, num_policies))
        ...     )
        ... }
        >>> # Simply pass the specificaiton and the number of desired records:
        >>> 
        >>> df_policies = make_market_basket(dict_policies, num_policies)
        >>>
        >>> print(df_policies)
            policy_id policy_classification  advance_shop_days  credit_score
        0            1                     R                 39         622.0
        1            2                     X                  7         751.0
        2            3                     I                 46         759.0
        3            4                     X                  2         696.0
        4            5                     I                 31         950.0
        ..         ...                   ...                ...           ...
        995        996                     I                 37         898.0
        996        997                     X                 25         804.0
        997        998                     X                 53           0.0
        998        999                     R                  9         745.0
        999       1000                     I                 32           1.0

        [1000 rows x 4 columns]

    </details>

</details>

<details>
<summary><b>Auto-join Datasets</b></summary>

The heart of the package lies in the custom class `SearchableDict`, where it can take any number of tables and provide a easy way to access information.

Suppose we have the following tables
    
    >>> print(df_policies)
    policy_id policy_classification  advance_shop_days  credit_score
    0            1                     I                 25         787.0
    1            2                     X                 21         805.0
    2            3                     I                 32         993.0
    3            4                     X                 10         725.0
    4            5                     R                 10         542.0
    ..         ...                   ...                ...           ...
    995        996                     R                 16           1.0
    996        997                     I                  3         627.0
    997        998                     R                 26         772.0
    998        999                     I                 52         847.0
    999       1000                     X                 28           1.0

    [1000 rows x 4 columns]

    >>> print(df_drivers)
    policy_id  driver_id  is_primary   age gender marital_status  months_experienced
    0             1          1        True  42.0      M              S                  38
    1             1          2       False  32.0      F              M                  36
    2             1          3       False  16.0      M              S                  31
    3             1          4       False  16.0      M              S                  12
    4             2          1        True  68.0      M              M                  20
    ...         ...        ...         ...   ...    ...            ...                 ...
    2533        999          1        True  25.0      M              S                  18
    2534        999          2       False  35.0      F              M                  13
    2535       1000          1        True  49.0      M              M                  49
    2536       1000          2       False  24.0      F              M                  18
    2537       1000          3       False  16.0      F              S                   4

    [2538 rows x 7 columns]

    >>> print(df_vehicles)
    policy_id  vehicle_id  model_year           recovery_device_type  COMP_deductible  COLL_deductible
    0             1           1        2012                           NONE              150              500
    1             1           2        1962  PASSIVE_ALARM_TRACKING_DEVICE             1500             9999
    2             1           3        1983                     ALARM_ONLY              500              100
    3             2           1        2016                  PASSIVE_ALARM              150             1500
    4             3           1        1983                           NONE             1000              150
    ...         ...         ...         ...                            ...              ...              ...
    2544        999           2        1973                  PASSIVE_ALARM             2000             1000
    2545        999           3        2012                TRACKING_DEVICE              150             9999
    2546       1000           1        1971  PASSIVE_ALARM_TRACKING_DEVICE              750             1500
    2547       1000           2        2008              NON_PASSIVE_ALARM             9999              150
    2548       1000           3        2016  PASSIVE_ALARM_TRACKING_DEVICE              150              750

    [2549 rows x 6 columns]

Simply load everything into a `SearchableDict` and we can easily access any attribute(s) from one or more tables:

    >>> market_basket = SearchableDict(
    ...     policies = df_policies,
    ...     drivers = df_drivers,
    ...     vehicles = df_vehicles
    ... )

    >>> # accessing whole tables
    >>> print(market_basket.drivers)
                            is_primary   age gender marital_status  months_experienced
    policy_id driver_id                                                            
    1           1                True  42.0      M              S                  38
                2               False  32.0      F              M                  36
                3               False  16.0      M              S                  31
                4               False  16.0      M              S                  12
    2           1                True  68.0      M              M                  20
    ...                         ...   ...    ...            ...                 ...
    999         1                True  25.0      M              S                  18
                2               False  35.0      F              M                  13
    1000        1                True  49.0      M              M                  49
                2               False  24.0      F              M                  18
                3               False  16.0      F              S                   4

    [2538 rows x 5 columns]
    
    >>> # accessing an attribute
    >>> print(market_basket.gender)
    policy_id  driver_id
    1           1            M
                2            F
                3            M
                4            M
    2           1            M
                        ..
    999         1            M
                2            F
    1000        1            M
                2            F
                3            F
    Name: gender, Length: 2538, dtype: object

    >>> # accessing multiple attributes
    >>> print(market_basket[['age', 'gender', 'marital_status']])
                        age gender marital_status
    policy_id driver_id                            
    1           1          42.0      M              S
                2          32.0      F              M
                3          16.0      M              S
                4          16.0      M              S
    2           1          68.0      M              M
    ...                   ...    ...            ...
    999         1          25.0      M              S
                2          35.0      F              M
    1000        1          49.0      M              M
                2          24.0      F              M
                3          16.0      F              S

    [2538 rows x 3 columns]

    >>> # accessing multiple attributes from MULTIPLE tables!
    >>> # note the new indices
    >>> print(market_basket[['policy_classification', 'model_year']])
                        policy_classification  model_year
    policy_id vehicle_id                                  
    1           1                              I        2012
                2                              I        1962
                3                              I        1983
    2           1                              X        2016
    3           1                              I        1983
    ...                                    ...         ...
    999         2                              I        1973
                3                              I        2012
    1000        1                              X        1971
                2                              X        2008
                3                              X        2016

    [2549 rows x 2 columns]

</details>
