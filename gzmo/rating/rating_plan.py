from abc import ABC
import warnings
import traceback
import graphlib
import multiprocessing as mp
import queue
from functools import partialmethod

import pandas as pd
import numpy as np

from gzmo.base import FancyDF, FancyDict, SearchableDict, AccessLogger
from gzmo.rating import helpers


class RatingPlan(FancyDict):
    """This class handles the initialization of a rating plan."""

    @classmethod
    def from_unprocessed_dataframes(cls, dict_unprocessed_dataframes: dict):
        rating_plan = cls()
        rating_plan.register_unprocessed_dataframes(
            **dict_unprocessed_dataframes)
        return rating_plan

    @classmethod
    def from_excel(cls, io: str, *args, **kwargs) -> None:
        """A wrapper of panda's read_excel function that
            - reads and prepare rating tables from the excel file
            - adds the tables to the rating plan.

        Args:
            See pandas.read_excel documentation for details.
        """
        # initialize rating plan
        rating_plan = cls()
        rating_plan.read_excel(io, *args, **kwargs)
        return rating_plan

    def register_unprocessed_dataframes(self, **unprocessed_dataframes):
        for table_name, table in unprocessed_dataframes.items():
            rating_table = LookupRatingTable.from_unprocessed_table(table)
            self.register(**{table_name: rating_table})

    def read_excel(self, io: str, *args, **kwargs):
        # read excel tables
        # Need to make sure sheet_name is either None or a list,
        # to make sure a dict is returned by pd.read_excel
        if (sheet_name := kwargs.pop('sheet_name', None)) is not None:
            if not isinstance(sheet_name, list):
                sheet_name = [sheet_name]
        default_excel_args = {
            'na_filter': False,
            'true_values': ['TRUE', 'True', 'true'],
            'false_values': ['FALSE', 'False', 'false'],
            'sheet_name': sheet_name,
        }
        excel_file = pd.read_excel(
            io,
            *args,
            **{**default_excel_args, **kwargs}
            )
        self.register_unprocessed_dataframes(**excel_file)
        # for table_name, table in excel_file.items():
        #     rating_table = LookupRatingTable.from_unprocessed_table(table)
        #     self.register(**{table_name: rating_table})
        return

    def make_dag(self):
        dependencies = {}
        for name_i, step_i in self.items():
            upstream = {
                name_j
                for name_j, step_j in self.items()
                if (set(step_i.inputs) & (set(step_j.outputs) | {name_j}))
                }
            dependencies[name_i] = upstream
        dag = graphlib.TopologicalSorter(dependencies)
        return dag

    def rate(self, book: SearchableDict, parallel = True):

        # Initialize FancyDict to store results
        session = SearchableDict()
        # rating_results will have a lot of attributes overlapping columns
        #   so we don't want these to be indices to be joined on.
        #   They will already have the appropriate index
        rating_results = SearchableDict(joinable_indices = False)
        # we want to search the rating results first
        # so we register that first
        session.register(**{
            'rating_results': rating_results,
            'book': book
        })
        try:
            if parallel:
                self._rate_parallel(session)
            else:
                self._rate_sequential(session)
        except:
            raise
        finally:
            return session

    # each worker will take rating steps from the queue to work on
    def _worker(self, queue_to_process, queue_results):
        while True:
            # get work
            work = queue_to_process.get()  # blocks
            # check for stop
            if work is None:
                break
            # unpack work
            rating_step_name, rating_step, session = work
            # do the work
            try:
                rating_step_result = rating_step.evaluate(session)
            except Exception as e:
                full_traceback = traceback.format_exc()
                # let the main process know if there is an error
                queue_results.put((rating_step_name, (e, full_traceback)))
            else:
                # put the results in the results queue
                queue_results.put((rating_step_name, rating_step_result))
                # signal that this task is completed
                queue_to_process.task_done()


    def _rate_parallel(self, session):

        def handle_worker_exceptions(e):
            raise e

        # make dag
        dag = self.make_dag()
        dag.prepare()

        # Create the shared queues to pass rating steps to workers to work on
        # and results back to the main process to load into the session
        manager = mp.Manager()
        queue_to_process = manager.JoinableQueue()
        queue_results = manager.Queue()

        # start the worker processes
        num_workers = mp.cpu_count()
        with mp.Pool(num_workers) as pool:
            for _ in range(num_workers):
                async_result = pool.apply_async(
                    self._worker,
                    args = (queue_to_process, queue_results),
                    error_callback = handle_worker_exceptions
                    )
            
            # dag is active when progress can be made:
            #   1) there are nodes ready not yet returned by `get_ready()`, or
            #   2) # nodes marked `done` < # nodes returned by `get_ready()`
            while dag.is_active():
                # `get_ready()`` returns all nodes that are ready
                for rating_step_name in dag.get_ready():
                    # add the rating step to the queue of rating steps to process
                    queue_to_process.put(
                        (rating_step_name, self[rating_step_name], session)
                        )
                try:
                    rating_step_name, rating_step_result = \
                        queue_results.get(timeout = 1)
                    if isinstance(rating_step_result, Exception):
                        e, full_traceback = rating_step_result
                        runtime_error = \
                            RuntimeError(f'Error when evaluating {rating_step_name}')
                        traceback.print_exception(
                            type(runtime_error),
                            runtime_error,
                            runtime_error.__traceback__
                            )
                        pool.terminate()
                        pool.join()
                        print(full_traceback)
                        raise e
                        # traceback.print_exception(type(e), e, e.__traceback__)
                        return
                    else:
                        session.rating_results.register(
                        **{rating_step_name: rating_step_result}
                        )
                        dag.done(rating_step_name)
                except queue.Empty:
                    pass
            
            # send poison pill to kill workers
            for _ in range(num_workers):
                queue_to_process.put(None)


    def _rate_sequential(self, session):
        # make dag
        dag = self.make_dag()
        for rating_step_name in dag.static_order():
                rating_step = self[rating_step_name]
                rating_step_result = rating_step.evaluate(session)
                session.rating_results.register(
                    **{rating_step_name: rating_step_result}
                    )

class RatingStep:

    def __init__(self, eval_func = None, inputs = None, outputs = None, **kwargs) -> None:
        # super().__init__()
        # if no inputs are passed, try to get it from eval_func
        if inputs:
            self.inputs = inputs
        elif eval_func:
            try:
                self.inputs = self.auto_get_inputs(eval_func)
            except:
                warnings.warn(
                    'Unable to auto-get inputs for rating step. ' + \
                    'Assuming empty input list'
                    )
                self.inputs = []
        else:
            self.inputs = []
        self.outputs = outputs or []
        self.evaluate = eval_func
    
    @staticmethod
    def auto_get_inputs(eval_func):
            access_logger = AccessLogger()
            # probably a bad practice, but this catch all
            # try-except block will allow us to get *something*
            # if the function call breaks in the middle
            # Example: if there is an unpacking action
            try:
                _ = eval_func(access_logger)
            except:
                pass
            accessed = [path[-1] for path in access_logger.accessed]
            return accessed
    

class BaseRatingTable(FancyDF, RatingStep, ABC):

    # _metadata define properties that will be passed to manipulation results.
    # https://pandas.pydata.org/docs/development/extending.html
    _metadata = [
        'name',
        'version',
        'effective_date',
        'serff_filing_number',
        'state_tracking_number',
        'company_tracking_number',
        'additional_info',
        'inputs',
        'outputs',
        'wildcard_characters',
        '_wildcard_markers'
    ]

    # defaults
    default_wildcard_characters = \
        ['*', pd.Interval(-np.inf, np.inf, closed = 'both')]

    def __init__(
            self,
            data: pd.DataFrame,
            inputs: list,
            outputs: list,
            wildcard_characters = None,
            **kwargs
            ) -> None:
        """Initializes a RatingTable instance.

        Args:
            name (str): The name of the rating table.
        """
        FancyDF.__init__(self, data = data.copy(), **kwargs)
        RatingStep.__init__(self, eval_func = self.evaluate, \
            inputs = inputs, outputs = outputs, **kwargs)
        
        # information about the rating table
        self.wildcard_characters = \
            wildcard_characters or BaseRatingTable.default_wildcard_characters
        
        # don't allow mixed type indices
        if isinstance(self.index, pd.MultiIndex):
            for lvl in self.index.levels:
                if lvl.dtype == 'O':
                    self.index = \
                        self.index.set_levels(lvl.astype(str), level = lvl.name)
        else:
            if self.index.dtype == 'O':
                self.index = self.index.astype(str)

        # Keep a marker of whether a row has any wildcards
        # For intervals, (-inf, inf) or (*, *) are treated as wildcards
        self._wildcard_markers = helpers.get_wildcard_markers(
            self.index.to_frame(),
            self.wildcard_characters
            )

        self._check_requirements()

    # to retain subclasses through pandas data manipulations
    # https://pandas.pydata.org/docs/development/extending.html
    # Also see https://github.com/pandas-dev/pandas/issues/19300
    @property
    def _constructor(self):
        return BaseRatingTable._internal_ctor

    @classmethod
    def _internal_ctor(cls, *args, **kwargs):
        kwargs['inputs'] = None
        kwargs['outputs'] = None
        return cls(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        return self.evaluate(*args, **kwargs)

    def _check_requirements(self):
        missing_outputs = set(self.outputs) - set(self.columns)
        assert (missing_outputs == set()), \
            f'Some outputs do not have a column: {missing_outputs}'

    @property
    def inputs(self):
        """ This ensures a modified RatingTable returns the correct inputs.

        NOTE: This is a "read-only" attribute, and any values set (to `inputs`)
                by the user will not be recorded.
        """        
        return [i for i in self.index.names if i is not None]

    @inputs.setter
    def inputs(self, value):
        pass
    
    @property
    def outputs(self):
        """ This ensures a modified RatingTable returns the correct outputs.

        NOTE: This is a "read-only" attribute, and any values set (to `outputs`)
                by the user will not be recorded.
        """   
        return list(self.columns)
    
    @outputs.setter
    def outputs(self, value):
        pass


    # TODO: use typing.overload for type hinting
    def evaluate(self, *args, **kwargs):
        # calls different methods based on input type.
        if args:
            if isinstance(args[0], (pd.DataFrame, FancyDict)):
                if (inputs := args[0].get(self.inputs).copy()) is None:
                    raise RuntimeError(f'Unable to get inputs.')
                else:
                    return self._eval_table(inputs)
            elif isinstance(args[0], dict):
                return self._eval_single(**args[0])
            else:
                return self._eval_single(*args)
        elif kwargs:
            return self._eval_single(**kwargs)
    
    def _eval_table(self, input_table):
        lst_dictoutputs = input_table.apply(self._eval_single, axis = 1).tolist()
        return FancyDF.from_records(lst_dictoutputs)
    
    #TODO: why doesn't this work?
    # @abstractmethod
    def _eval_single(self, *args, **kwargs):
        pass
    
    @classmethod
    def from_unprocessed_table(cls, df, **kwargs):
        processed_rating_table, inputs, outputs = \
            helpers.process_rating_table(df)
        return cls(processed_rating_table, inputs, outputs, **kwargs)

class LookupRatingTable(BaseRatingTable):

    def _eval_table(self, input_table):
        # The function to handle dataframe inputs
        # first do _eval_reindex on non-wildcard rows of self for all rows in input_table
        # if fails, just fill in with None
        # then do eval_match on wildcard rows on any rows that do not yet have a value
        
        # if there are no inputs, simply do a cross join
        if self.inputs == []:
            assert len(self) == 1, \
                f'Table has no inputs but has more than 1 row.'
            empty_dataframe = FancyDF(index = input_table.index)
            ret = empty_dataframe \
                .merge(
                    self,
                    how = 'cross'
                    )
            # pandas merge does not preserve index on cross joins
            ret.index = input_table.index
            return ret

        # We don't allow mixed types--if an input is mixed-typed, it is a string
        #   (See BaseRatingTable.__init__)
        # Cast the input table's column to string type if so
        for i in self.inputs:
            if self.index.get_level_values(i).dtype == 'O':
                input_table.loc[:, i] = input_table[i].astype(str).values
        # first use _eval_reindex on non-wildcard rows of self
        lookup_table_nonwildcard = self.loc[(~self._wildcard_markers).values]
        # need to remove the unused levels becuase they somehow mess up reindexing
        # See https://pandas.pydata.org/docs/user_guide/advanced.html#defined-levels
        if isinstance(lookup_table_nonwildcard.index, pd.MultiIndex):
            lookup_table_nonwildcard.index= \
                lookup_table_nonwildcard.index.remove_unused_levels()
        try:
            res_nonwildcard = self._eval_reindex(
                input_table, lookup_table = lookup_table_nonwildcard)
        # if reindex throws an error, go straight to using _eval_match
        except:
            res = self._eval_match(
                input_table, lookup_table = self)
            return res
        else:
            # use self._eval_match to match on wildcard rows
            lookup_table_wildcard = self.loc[self._wildcard_markers]
            isna = res_nonwildcard.isna().all(axis = 1)
            to_lookup = input_table.loc[isna]
            if (len(to_lookup) > 0) and (len(lookup_table_wildcard) > 0):
                res_wildcard = self._eval_match(
                    to_lookup, lookup_table = lookup_table_wildcard)
                # combine res_nonwildcard and res_wildcard
                res = res_nonwildcard.fillna(res_wildcard)
            else:
                res = res_nonwildcard
            # res = pd.concat([res_nonwildcard, res_wildcard], axis = 0, ignore_index = False)
            # reorder to original index
            res = res.loc[input_table.index]
            return res
        

    def _eval_reindex(self, input_table, lookup_table = None):
        """Uses df.reindex to look up rows that match 
            each row of inputs in input_table.

        Args:
            input_table (pd.DataFrame): A Pandas DataFrame containing the
                inputs to be processed, with columns named like self.inputs.
            lookup_table: The dataframe in which to lookup rows.

        Returns:
            FancyDF: Rows in `self` with index matching the inputs.
        
        Raises:
            ValueError: pandas multiindices do not play well with overlapping
                intervals in the IntervalIndex, even if the multiindex itself
                is unique. If there are rows with overlapping intervals,
                may raise:
                `ValueError: setting an array element with a sequence.`
        
        NOTE:
                There is currently a bug on this:
                https://github.com/pandas-dev/pandas/issues/46699
                Right now if this error occurs, _eval_table should reroute the
                call to _eval_match, which is more robust but slower.
                Obviously this only happens if _eval_reindex is called
                within _eval_table.
                
        """

        # reorder input columns
        passed_inputs = input_table.loc[:, self.inputs]

        # Define the lookup table
        if lookup_table is None:
            lookup_table = self
        # perform the lookup
        inputs = passed_inputs.to_records(index = False).tolist()
        results = lookup_table.reindex(
            index = inputs, columns = self.outputs, copy = True)
        # use the inputs' index
        # We also want to create a new dataframe, otherwise the resulting
        # object would be a RatingTable instance.
        results = FancyDF(
            results.to_dict('list'),    # need to drop the original index
            index = passed_inputs.index
            )
        return results

    def _eval_match(self, input_table, lookup_table = None):
        """Uses self._lookup to look up rows that match 
            each row of inputs in input_table.

        Args:
            input_table (pd.DataFrame): A Pandas DataFrame containing the
                inputs to be processed, with columns named like self.inputs.
            lookup_df: The dataframe in which to lookup rows.

        Returns:
            pd.DataFrame: Rows in `self` with index matching the inputs.
        """

        # Define the lookup table
        if lookup_table is None:
            lookup_table = self
        records = input_table.apply(
            lambda row: self._eval_single(
                lookup_table = lookup_table, **row.to_dict()
                ),
            axis = 1
            )
        records = records.tolist()
        # keep original index
        result = FancyDF.from_records(records, index = input_table.index)
        
        return result

    def _eval_single(self, *args, lookup_table = None, **kwargs):
        """Function to handle a single (set of) input.
        Pass one set of inputs to retrieve a mathching row as a dict.

        Args:
            *args: Inputs passed as non-keyword arguments must match the
                order of `self.inputs`. Keyword arguments are ignored
                if any non-keyword arguments are passed.
            lookup_table: The dataframe in which to lookup rows.
            **args: Inputs passed as keyword arguments must match
                `self.inputs` exactly. Keyword arguments are ignored
                if any non-keyword arguments are passed.
        Returns:
            dict: {input name: output value}
        """
        
        # Define the lookup table
        if lookup_table is None:
            lookup_table = self

        # if there are no inputs, simply return the (first) row
        if self.inputs == []:
            return lookup_table.iloc[0].to_dict()

        # First convert non-keyword arguments to a dict
        if args:
            passed_inputs = {k: v for k, v in zip(self.inputs, args)}
        else:
            passed_inputs = {**kwargs}


        # Find out what rows match the given inputs
        matches = []
        for input_name in self.inputs:
            passed_input = passed_inputs[input_name]
            lookup_column = lookup_table.index.get_level_values(input_name)
            if isinstance(lookup_column, pd.IntervalIndex):
                matched = lookup_column.contains(passed_input)
            else:
                matched = lookup_column.isin(
                    self.wildcard_characters + [passed_input]
                )
            matches.append(matched)
        
        matching_rows_filter_w_wildcards = \
            np.all(matches, axis = 0)
        matching_rows_filter_wo_wildcards = \
            matching_rows_filter_w_wildcards & ((~self._wildcard_markers).values)
        if matching_rows_filter_wo_wildcards.sum() == 0:
            matching_rows_filter = matching_rows_filter_w_wildcards
        else:
            matching_rows_filter = matching_rows_filter_wo_wildcards
        
        matching_rows = lookup_table.loc[matching_rows_filter]
        if len(matching_rows) == 0:
            return {k: None for k in self.outputs}
        else:
            return matching_rows.iloc[0].to_dict()

class InterpolatedRatingTable(BaseRatingTable):
    
    def __init__(self, data, inputs, outputs, **kwargs) -> None:
        super().__init__(data, inputs, outputs, **kwargs)
        # flatten multiindex to single level index for interpolation
        # __init__ will have called _check_requirements,
        #   which makes sure there is only 1 level of index
        self.index = pd.Index(self.index.get_level_values(0))

    @classmethod
    def from_rating_table(cls, rating_table, **kwargs):
        # initialize a new instance
        inputs = rating_table.inputs
        outputs = rating_table.outputs
        rating_table.index = pd.Index(rating_table.index.get_level_values(0))
        ret = cls(rating_table, inputs, outputs)
        # inherit metadata
        ret.__dict__.update({
            k: v for k, v in rating_table.__dict__.items()
            if k in rating_table._metadata
            })
        return ret

    def _check_requirements(self):
       
        super()._check_requirements()

        assert not self._wildcard_markers.any(), \
            'Tables with wildcard markers cannot be interpolated.'
        assert (len(self.inputs) == 1) and (len(self.index.names) == 1), \
            'Only tables with 1 input can be interpolated.'

    def make_lookup_table(self, passed_inputs):
        input_set = set(passed_inputs) | set(self.index)
        expanded_table = self.reindex(list(input_set))
        expanded_table = expanded_table.sort_index()
        
        # interpolate
        kw = dict(method='from_derivatives', order = 1, extrapolate = True, fill_value="extrapolate", limit_direction="both")
        # kw = dict(method="spline", order = 1, fill_value="extrapolate", limit_direction="both")
        lookup_table = expanded_table.interpolate(**kw)

        return lookup_table

    def _eval_table(self, input_table):
        # reorder input columns
        passed_inputs = input_table.loc[:, self.inputs]

        #  Define the lookup table
        lookup_table = \
            self.make_lookup_table(set(passed_inputs.values.reshape(-1)))
        # perform the lookup
        # inputs = passed_inputs.to_records(index = False).tolist()
        inputs = passed_inputs.values.reshape(-1)
        results = lookup_table.reindex(
            index = inputs, columns = self.outputs, copy = True)
        # use the inputs' index
        # We also want to create a new dataframe, otherwise the resulting
        # object would be a RatingTable instance.
        results = FancyDF(results)
        results.index = passed_inputs.index

        return results
    
    def _eval_single(self, *args, **kwargs):
        
        # First convert non-keyword arguments to a dict
        if args:
            assert len(args) == 1, 'Too many inputs for an interpolated table.'
            passed_input = args[0]
        else:
            passed_input = kwargs[self.inputs[0]]
        
        #  Define the lookup table
        lookup_table = self.make_lookup_table([passed_input])

        return lookup_table.loc[passed_input, self.outputs].to_dict()