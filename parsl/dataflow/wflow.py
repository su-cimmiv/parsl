import atexit
import logging
import os
import pathlib
import pickle
import random
import time
import typeguard
import inspect
import threading
import sys
import datetime
from getpass import getuser
from typing import Any, Dict, List, Optional, Sequence
from uuid import uuid4
from socket import gethostname
from concurrent.futures import Future
from functools import partial

import parsl
from parsl.app.errors import RemoteExceptionWrapper
from parsl.app.futures import DataFuture
from parsl.config import Config
from parsl.data_provider.data_manager import DataManager
from parsl.data_provider.files import File
from parsl.dataflow.error import BadCheckpoint, ConfigurationError, DependencyError, DuplicateTaskError
from parsl.dataflow.flow_control import FlowControl, Timer
from parsl.dataflow.futures import AppFuture
from parsl.dataflow.memoization import Memoizer
from parsl.dataflow.rundirs import make_rundir
from parsl.dataflow.states import States, FINAL_STATES, FINAL_FAILURE_STATES
from parsl.dataflow.usage_tracking.usage import UsageTracker
from parsl.executors.threads import ThreadPoolExecutor
from parsl.providers.provider_base import JobStatus, JobState
from parsl.utils import get_version, get_std_fname_mode, get_all_checkpoints

from parsl.monitoring.message_type import MessageType

from parsl.dataflow.dflow import DataFlowKernel

logger = logging.getLogger(__name__)


class Workflow(DataFlowKernel):
    
    SCHEMA = "workflow://"

    def _count_deps(self, depends):
        """Internal.

        Count the number of unresolved futures in the list depends.
        """
        count = 0
        for dep in depends:
            if isinstance(dep, Future):
                if not dep.done():
                    count += 1

        return count


    def wipe_task(self, task_id):
        """ Remove task with task_id from the internal tasks table
        """
        #if self.config.garbage_collect:
            ##del self.tasks[task_id]
    
    def find_task_working_directory(self, app_name):
        """Find the working directory of the task the current task depends on
        We know that to the sandbox_app are passed strings containing workflow:// schema
        Assuming that a workflow_app_name is associated  to the sandbox_app when
        the method submit is invoked, we can find working_directory by studying the task dict"""
        
        working_directory = ""  
        #For each key-value pair in tasks                    
        for key, value in self.tasks.items():
            #value in this case is still a key of an internal dict
            # if the value of 'workflow_app_name' is equal to the app_name passed                           
            if value['workflow_app_name'] == app_name:
                #now we are able to know the name of the app's working_directory on which the current app depends
                working_directory = value['app_fu'].result()['working_directory']
                self.tasks[key]['working_directory'] = working_directory
                break
        return working_directory

    def _add_input_deps(self, executor, args, kwargs, func):
        """Look for inputs of the app that are files. Give the data manager
        the opportunity to replace a file with a data future for that file,
        for example wrapping the result of a staging action.

        Args:
            - executor (str) : executor where the app is going to be launched
            - args (List) : Positional args to app function
            - kwargs (Dict) : Kwargs to app function
        """

        # Return if the task is a data management task, rather than doing
        #  data management on it.
        if self.check_staging_inhibited(kwargs):
            logger.debug("Not performing input staging")
            return args, kwargs, func

        inputs = kwargs.get('inputs', [])
        
        ''' for each workflow:// schema, find the working directory of the file and return the file path'''
        
        for i in range(len(inputs)):
            #replace workflow:// from the input
            inFile = inputs[i].replace(self.SCHEMA,"")
            #split by / the result of replacment, app_name is in 0
            info = inFile.split("/")
            app_name = info[0]
            working_directory = self.find_task_working_directory(app_name) #the name of the wd
            inFile = inFile.replace(app_name,working_directory) #replace workflow_app_name with the wd path
            inputs[i] = inFile
 
        
        for idx, f in enumerate(inputs):
            (inputs[idx], func) = self.data_manager.optionally_stage_in(f, func, executor)

        for kwarg, f in kwargs.items():
            (kwargs[kwarg], func) = self.data_manager.optionally_stage_in(f, func, executor)

        newargs = list(args)
        for idx, f in enumerate(newargs):
            (newargs[idx], func) = self.data_manager.optionally_stage_in(f, func, executor)

        return tuple(newargs), kwargs, func

    def _gather_all_deps(self, args: Sequence[Any], kwargs: Dict[str, Any]) -> List[Future]:
        """Assemble a list of all Futures passed as arguments, kwargs or in the inputs kwarg.

        Args:
            - args: The list of args pass to the app
            - kwargs: The dict of all kwargs passed to the app

        Returns:
            - list of dependencies

        """
        depends: List[Future] = []

        def check_dep(d):
            if isinstance(d, Future):
                depends.extend([d])

        #find the future associated to the task with a certain working_directory
        def find_task_fut(wd):
            d = None
            for key, value in self.tasks.items():                            
                if value['working_directory'] == wd:
                    d = value['app_fu']
                    break
            return d

        # Check the positional args
        for dep in args:
            check_dep(dep)

        # Check for explicit kwargs ex, fu_1=<fut>
        for key in kwargs:
            dep = kwargs[key]
            check_dep(dep)

        # Check for futures in inputs=[<fut>...]
        for dep in kwargs.get('inputs', []):
            #split by / the result of replacment, working_directory is in 0
            info = dep.split("/")
            wd = info[0]
            check_dep(find_task_fut(wd))

        return depends

    
    def submit(self, func, app_args, executors='all', cache=False, ignore_for_cache=None, app_kwargs={}, join=False):
        
        if ignore_for_cache is None:
            ignore_for_cache = []

        if self.cleanup_called:
            raise ValueError("Cannot submit to a DFK that has been cleaned up")

        task_id = self.task_count
        self.task_count += 1
        if isinstance(executors, str) and executors.lower() == 'all':
            choices = list(e for e in self.executors if e != '_parsl_internal')
        elif isinstance(executors, list):
            choices = executors
        else:
            raise ValueError("Task {} supplied invalid type for executors: {}".format(task_id, type(executors)))
        executor = random.choice(choices)
        logger.debug("Task {} will be sent to executor {}".format(task_id, executor))

        # The below uses func.__name__ before it has been wrapped by any staging code.

        label = app_kwargs.get('label')
        for kw in ['stdout', 'stderr']:
            if kw in app_kwargs:
                if app_kwargs[kw] == parsl.AUTO_LOGNAME:
                    if kw not in ignore_for_cache:
                        ignore_for_cache += [kw]
                    app_kwargs[kw] = os.path.join(
                                self.run_dir,
                                'task_logs',
                                str(int(task_id / 10000)).zfill(4),  # limit logs to 10k entries per directory
                                'task_{}_{}{}.{}'.format(
                                    str(task_id).zfill(4),
                                    func.__name__,
                                    '' if label is None else '_{}'.format(label),
                                    kw)
                    )

        resource_specification = app_kwargs.get('parsl_resource_specification', {})

        workflow_app_name = ""

        if "workflow_app_name" in app_kwargs:
            workflow_app_name = app_kwargs.get("workflow_app_name","")
        if workflow_app_name == "":
            workflow_app_name = func.__name__[0] + "-"+str(task_id)
            
        task_def = {'depends': None,
                    'executor': executor,
                    'func_name': func.__name__,
                    'memoize': cache,
                    'hashsum': None,
                    'exec_fu': None,
                    'fail_count': 0,
                    'fail_history': [],
                    'from_memo': None,
                    'ignore_for_cache': ignore_for_cache,
                    'join': join,
                    'joins': None,
                    'status': States.unsched,
                    'try_id': 0,
                    'id': task_id,
                    'time_invoked': datetime.datetime.now(),
                    'time_returned': None,
                    'try_time_launched': None,
                    'try_time_returned': None,
                    'resource_specification': resource_specification,
                    'workflow_app_name':workflow_app_name, #Added
                    'workflow_schema':None, #Added
                    'working_directory':None #Added 
                    }


        app_fu = AppFuture(task_def)

        # Transform remote input files to data futures
        app_args, app_kwargs, func = self._add_input_deps(executor, app_args, app_kwargs, func)

        func = self._add_output_deps(executor, app_args, app_kwargs, app_fu, func)

        #update the kwarg workflow_app_name
        app_kwargs['workflow_app_name']=workflow_app_name

        task_def.update({
                    'args': app_args,
                    'func': func,
                    'kwargs': app_kwargs,
                    'app_fu': app_fu})

        if task_id in self.tasks:
            raise DuplicateTaskError(
                "internal consistency error: Task {0} already exists in task list".format(task_id))
        else:
            self.tasks[task_id] = task_def

        # Get the list of dependencies for the task
        depends = self._gather_all_deps(app_args, app_kwargs)
        task_def['depends'] = depends

        depend_descs = []
        for d in depends:
            if isinstance(d, AppFuture) or isinstance(d, DataFuture):
                depend_descs.append("task {}".format(d.tid))
            else:
                depend_descs.append(repr(d))

        if depend_descs != []:
            waiting_message = "waiting on {}".format(", ".join(depend_descs))
        else:
            waiting_message = "not waiting on any dependency"

        logger.info("Task {} submitted for App {}, {}".format(task_id,
                                                              task_def['func_name'],
                                                              waiting_message))

        task_def['task_launch_lock'] = threading.Lock()

        app_fu.add_done_callback(partial(self.handle_app_update, task_id))
        task_def['status'] = States.pending
        logger.debug("Task {} set to pending state with AppFuture: {}".format(task_id, task_def['app_fu']))

        self._send_task_log_info(task_def)

        # at this point add callbacks to all dependencies to do a launch_if_ready
        # call whenever a dependency completes.

        # we need to be careful about the order of setting the state to pending,
        # adding the callbacks, and caling launch_if_ready explicitly once always below.

        # I think as long as we call launch_if_ready once after setting pending, then
        # we can add the callback dependencies at any point: if the callbacks all fire
        # before then, they won't cause a launch, but the one below will. if they fire
        # after we set it pending, then the last one will cause a launch, and the
        # explicit one won't.

        for d in depends:

            def callback_adapter(dep_fut):
                self.launch_if_ready(task_id)

            try:
                d.add_done_callback(callback_adapter)
            except Exception as e:
                logger.error("add_done_callback got an exception {} which will be ignored".format(e))

        self.launch_if_ready(task_id)

        return app_fu

class WorkflowLoader(object):
    """Manage which DataFlowKernel is active.

    This is a singleton class containing only class methods. You should not
    need to instantiate this class.
    """

    _dfk = None
    _workflow_name = None
    _scratch_dir_base = None

    @classmethod
    def clear(cls):
        """Clear the active DataFlowKernel so that a new one can be loaded."""
        cls._dfk = None

    @classmethod
    @typeguard.typechecked
    def load(cls, config: Optional[Config] = None):
        """Load a DataFlowKernel.

        Args:
            - config (Config) : Configuration to load. This config will be passed to a
              new DataFlowKernel instantiation which will be set as the active DataFlowKernel.
        Returns:
            - DataFlowKernel : The loaded DataFlowKernel object.
        """
        if cls._dfk is not None:
            raise RuntimeError('Config has already been loaded')

        if config is None:
            cls._dfk = Workflow(Config())
        else:
            cls._dfk = Workflow(config)

        return cls._dfk

    @classmethod
    def wait_for_current_tasks(cls):
        """Waits for all tasks in the task list to be completed, by waiting for their
        AppFuture to be completed. This method will not necessarily wait for any tasks
        added after cleanup has started such as data stageout.
        """
        cls.dfk().wait_for_current_tasks()

    @classmethod
    def dfk(cls):
        """Return the currently-loaded DataFlowKernel."""
        if cls._dfk is None:
            raise RuntimeError('Must first load config')
        return cls._dfk
    
    @classmethod
    def scratch_dir_base(cls):
        if cls._scratch_dir_base == None:
            cls._scratch_dir_base = os.getcwd()
        return cls._scratch_dir_base
    @classmethod
    def workflow_name_setter(cls,value):
        cls._workflow_name = value
    @classmethod
    def workflow_name(cls):
        if cls._workflow_name is None:
            cls._workflow_name = ""
        return cls._workflow_name

